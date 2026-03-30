using InterBus.Extensions;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.PostgreSql;
using Xunit;

namespace InterBus.PostgreSQL.Tests;

/// <summary>
/// Integration tests for the PostgreSQL transport. Each test class instance shares
/// a single PostgreSQL container spun up via Testcontainers.
/// </summary>
[Collection("PostgreSQL")]
public sealed class PublishConsumeTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _pgContainer = new PostgreSqlBuilder("postgres:18-alpine")
        .Build();

    public ValueTask InitializeAsync() => new(_pgContainer.StartAsync());
    public ValueTask DisposeAsync() => _pgContainer.DisposeAsync();

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Polls <paramref name="condition"/> up to <paramref name="timeoutMs"/> milliseconds,
    /// checking every <paramref name="intervalMs"/> milliseconds.
    /// </summary>
    private static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 10_000,
        int intervalMs = 50)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

    /// <summary>
    /// Builds a minimal dispatch delegate that deserialises a payload into
    /// <typeparamref name="T"/> and calls <see cref="IConsumer{T}.Consume"/>.
    /// </summary>
    private static Func<IServiceProvider, byte[], IConsumeContext, CancellationToken, Task>
        BuildDispatch<TConsumer, T>()
        where TConsumer : class, IConsumer<T>
        where T : class
    {
        return static (sp, payload, context, ct) =>
        {
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var consumer = sp.GetRequiredService<TConsumer>();
            var message = serializer.Deserialize<T>(payload)!;
            return consumer.Consume(message, context, ct);
        };
    }

    private async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartAsync(
        Action<IServiceCollection> registerSingletons,
        Action<IInterBusBuilder> configureBus)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        registerSingletons(services);
        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return (provider, cts);
    }

    private static async Task StopAsync(ServiceProvider provider, CancellationTokenSource cts)
    {
        cts.Cancel();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try { await svc.StopAsync(CancellationToken.None); }
            catch (OperationCanceledException) { }
        }

        await provider.DisposeAsync();
        cts.Dispose();
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Publish_SingleConsumer_MessageIsDelivered()
    {
        var tracker = new OrderPlacedTracker();

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(tracker),
            bus =>
            {
                bus.AddPostgreSqlTransport(o => o.ConnectionString = _pgContainer.GetConnectionString());
                bus.AddConsumer<OrderPlacedConsumer>(
                    [typeof(OrderPlaced)],
                    BuildDispatch<OrderPlacedConsumer, OrderPlaced>());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "test@example.com", 99.99m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => tracker.Messages.Count == 1);

            Assert.Single(tracker.Messages);
            Assert.Equal(msg.OrderId, tracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Publish_MultipleConsumers_AllReceiveMessage()
    {
        var orderTracker = new OrderPlacedTracker();
        var auditTracker = new AuditTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(orderTracker);
                services.AddSingleton(auditTracker);
            },
            bus =>
            {
                bus.AddPostgreSqlTransport(o => o.ConnectionString = _pgContainer.GetConnectionString());
                bus.AddConsumer<OrderPlacedConsumer>(
                    [typeof(OrderPlaced)],
                    BuildDispatch<OrderPlacedConsumer, OrderPlaced>());
                bus.AddConsumer<AuditConsumer>(
                    [typeof(OrderPlaced)],
                    BuildDispatch<AuditConsumer, OrderPlaced>());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "fan@out.test", 50m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1 && auditTracker.Messages.Count == 1);

            Assert.Single(orderTracker.Messages);
            Assert.Equal(msg.OrderId, orderTracker.Messages.Single().OrderId);
            Assert.Single(auditTracker.Messages);
            Assert.Equal(msg.OrderId, auditTracker.Messages.Single().OrderId);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task Send_DirectToQueue_OnlyTargetConsumerReceives()
    {
        var orderTracker = new OrderPlacedTracker();
        var auditTracker = new AuditTracker();

        var (provider, cts) = await StartAsync(
            services =>
            {
                services.AddSingleton(orderTracker);
                services.AddSingleton(auditTracker);
            },
            bus =>
            {
                bus.AddPostgreSqlTransport(o => o.ConnectionString = _pgContainer.GetConnectionString());
                bus.AddConsumer<OrderPlacedConsumer>(
                    [typeof(OrderPlaced)],
                    BuildDispatch<OrderPlacedConsumer, OrderPlaced>());
                bus.AddConsumer<AuditConsumer>(
                    [typeof(OrderPlaced)],
                    BuildDispatch<AuditConsumer, OrderPlaced>());
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "direct@send.test", 25m);

            // Send directly to OrderPlacedConsumer only; AuditConsumer should NOT receive.
            await publisher.Send<OrderPlacedConsumer, OrderPlaced>(msg, TestContext.Current.CancellationToken);

            await WaitUntilAsync(() => orderTracker.Messages.Count == 1);

            Assert.Single(orderTracker.Messages);
            Assert.Equal(msg.OrderId, orderTracker.Messages.Single().OrderId);
            Assert.Empty(auditTracker.Messages);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task SchemaIsCreatedAutomatically()
    {
        var (provider, cts) = await StartAsync(
            _ => { },
            bus => bus.AddPostgreSqlTransport(o => o.ConnectionString = _pgContainer.GetConnectionString()));

        try
        {
            await using var dataSource = Npgsql.NpgsqlDataSource.Create(_pgContainer.GetConnectionString());
            await using var conn = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_schema = 'interbus' AND table_name IN ('messages', 'message_payloads', 'subscriptions', 'queues', 'topics')";
            var count = (long)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
            Assert.Equal(5L, count);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task SchemaMigration_OptOut_DoesNotRunMigration()
    {
        // Pre-create the normalised schema and tables manually (simulating an external migration tool).
        await using var setupDataSource = Npgsql.NpgsqlDataSource.Create(_pgContainer.GetConnectionString());
        await using var setupConn = await setupDataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
        await using var setupCmd = setupConn.CreateCommand();
        setupCmd.CommandText =
            "CREATE SCHEMA IF NOT EXISTS interbus;\n" +
            "CREATE TABLE IF NOT EXISTS interbus.queues (\n" +
            "    id         BIGSERIAL PRIMARY KEY,\n" +
            "    queue_name TEXT      NOT NULL UNIQUE\n" +
            ");\n" +
            "CREATE TABLE IF NOT EXISTS interbus.topics (\n" +
            "    id         BIGSERIAL PRIMARY KEY,\n" +
            "    topic_name TEXT      NOT NULL UNIQUE\n" +
            ");\n" +
            "CREATE TABLE IF NOT EXISTS interbus.message_payloads (\n" +
            "    id         BIGSERIAL PRIMARY KEY,\n" +
            "    payload    BYTEA       NOT NULL,\n" +
            "    headers    TEXT        NOT NULL,\n" +
            "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n" +
            ");\n" +
            "CREATE TABLE IF NOT EXISTS interbus.messages (\n" +
            "    id         BIGSERIAL   PRIMARY KEY,\n" +
            "    queue_id   BIGINT      NOT NULL REFERENCES interbus.queues(id),\n" +
            "    payload_id BIGINT      NOT NULL REFERENCES interbus.message_payloads(id),\n" +
            "    visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n" +
            "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n" +
            ");\n" +
            "CREATE INDEX IF NOT EXISTS ix_messages_queue_pending\n" +
            "    ON interbus.messages (queue_id, visible_at, id);\n" +
            "CREATE INDEX IF NOT EXISTS ix_messages_payload_id\n" +
            "    ON interbus.messages (payload_id);\n" +
            "CREATE TABLE IF NOT EXISTS interbus.subscriptions (\n" +
            "    queue_id   BIGINT NOT NULL REFERENCES interbus.queues(id),\n" +
            "    topic_id   BIGINT NOT NULL REFERENCES interbus.topics(id),\n" +
            "    PRIMARY KEY (queue_id, topic_id)\n" +
            ");";
        await setupCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Start the bus with schema migrations disabled — should not throw.
        var (provider, cts) = await StartAsync(
            _ => { },
            bus => bus.AddPostgreSqlTransport(o =>
            {
                o.ConnectionString = _pgContainer.GetConnectionString();
                o.RunSchemaMigrations = false;
            }));

        try
        {
            // Schema still exists because we created it manually above.
            await using var dataSource = Npgsql.NpgsqlDataSource.Create(_pgContainer.GetConnectionString());
            await using var conn = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText =
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_schema = 'interbus' AND table_name IN ('messages', 'message_payloads', 'subscriptions', 'queues', 'topics')";
            var count = (long)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
            Assert.Equal(5L, count);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task FailedConsumer_MessageIsRetried()
    {
        var tracker = new OrderPlacedTracker();
        var callCount = 0;

        var (provider, cts) = await StartAsync(
            services => services.AddSingleton(tracker),
            bus =>
            {
                bus.AddPostgreSqlTransport(o =>
                {
                    o.ConnectionString = _pgContainer.GetConnectionString();
                    // Short fallback interval so the retry fires quickly in this test.
                    o.FallbackPollInterval = TimeSpan.FromSeconds(2);
                });
                bus.AddConsumer<OrderPlacedConsumer>(
                    [typeof(OrderPlaced)],
                    (sp, payload, context, ct) =>
                    {
                        Interlocked.Increment(ref callCount);
                        if (callCount < 2)
                            throw new InvalidOperationException("transient failure");

                        // Second attempt succeeds.
                        var serializer = sp.GetRequiredService<IMessageSerializer>();
                        var consumer = sp.GetRequiredService<OrderPlacedConsumer>();
                        var message = serializer.Deserialize<OrderPlaced>(payload)!;
                        return consumer.Consume(message, context, ct);
                    });
            });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
            var msg = new OrderPlaced(Guid.NewGuid(), "retry@test.com", 10m);
            await publisher.Publish(msg, TestContext.Current.CancellationToken);

            // Wait for the second (successful) delivery.
            await WaitUntilAsync(() => tracker.Messages.Count == 1, timeoutMs: 15_000);

            Assert.Single(tracker.Messages);
            Assert.Equal(msg.OrderId, tracker.Messages.Single().OrderId);
            Assert.True(callCount >= 2, $"Expected at least 2 dispatch attempts, got {callCount}");
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }
}
