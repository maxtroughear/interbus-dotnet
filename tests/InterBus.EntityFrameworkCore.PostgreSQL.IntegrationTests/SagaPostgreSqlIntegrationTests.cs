using InterBus.EntityFrameworkCore.Extensions;
using InterBus.EntityFrameworkCore.LockProviders;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.PostgreSql;

namespace InterBus.EntityFrameworkCore.PostgreSQL.IntegrationTests;

// ---------------------------------------------------------------------------
// Saga messages / state / sagas for PostgreSQL integration tests
// ---------------------------------------------------------------------------

public record PgSagaOrderStarted(Guid OrderId, string CustomerEmail);

public record PgSagaOrderConfirmed(Guid OrderId);

public sealed class PgSagaOrderState
{
    public string? CustomerEmail { get; set; }
    public bool IsConfirmed { get; set; }
    public int MessageCount { get; set; }
}

public sealed class PgSagaOrderSaga :
    ISagaConsumer<PgSagaOrderState, PgSagaOrderStarted>,
    ISagaConsumer<PgSagaOrderState, PgSagaOrderConfirmed>
{
    public string GetCorrelationId(PgSagaOrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(PgSagaOrderConfirmed message) => message.OrderId.ToString();

    public Task Consume(PgSagaOrderStarted message, ISagaContext<PgSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(PgSagaOrderConfirmed message, ISagaContext<PgSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}

/// <summary>
/// Integration tests for the EF Core saga repository using a real PostgreSQL database.
/// Validates that saga state is persisted correctly and that the
/// <see cref="PostgreSqlLockProvider{TDbContext}"/> correctly serialises concurrent
/// access to the same saga instance.
/// </summary>
[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public class SagaPostgreSqlIntegrationTests(string imageTag)
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder(imageTag).Build();
    private ServiceProvider _provider = null!;
    private CancellationTokenSource _cts = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp() => await _container.StartAsync();

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

    [SetUp]
    public async Task SetUp()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddDbContext<SagaTestDbContext>(o =>
            o.UseNpgsql(_container.GetConnectionString()));

        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddEfCore<SagaTestDbContext>(ef => ef.UsePostgreSql());
            bus.AddSaga<PgSagaOrderSaga, PgSagaOrderState>(saga =>
            {
                saga.UseEfCoreRepository<PgSagaOrderSaga, PgSagaOrderState, SagaTestDbContext>();
                // saga.UseEfCoreInboxOutbox<SagaTestDbContext>();
            });
        });

        _provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();

        await using var scope = _provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<SagaTestDbContext>();
        await db.Database.EnsureCreatedAsync();
        await db.SagaStateEntries.ExecuteDeleteAsync();

        foreach (var svc in _provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _cts.CancelAsync();
        foreach (var svc in _provider.GetServices<IHostedService>())
        {
            try
            {
                await svc.StopAsync(CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
            }
        }

        await _provider.DisposeAsync();
        _cts.Dispose();
    }

    private async Task<SagaEntry<PgSagaOrderState>?> GetEntryAsync(string correlationId)
    {
        await using var scope = _provider.CreateAsyncScope();
        var repo = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<PgSagaOrderSaga, PgSagaOrderState>>();
        return await repo.GetAsync(correlationId);
    }

    [Test]
    public async Task Saga_SingleMessage_StateIsPersisted()
    {
        var orderId = Guid.NewGuid();

        await using var scope = _provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        await publisher.PublishAsync(new PgSagaOrderStarted(orderId, "pg@test.com"));

        await WaitUntilAsync(async () =>
            (await GetEntryAsync(orderId.ToString()))?.State.MessageCount == 1);

        var entry = await GetEntryAsync(orderId.ToString());

        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.State.CustomerEmail, Is.EqualTo("pg@test.com"));
        Assert.That(entry.State.MessageCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Saga_MultipleMessages_StateAccumulates()
    {
        var orderId = Guid.NewGuid();

        await using var scope = _provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        await publisher.PublishAsync(new PgSagaOrderStarted(orderId, "order@pg.com"));
        await publisher.PublishAsync(new PgSagaOrderConfirmed(orderId));

        await WaitUntilAsync(async () =>
            (await GetEntryAsync(orderId.ToString()))?.State.MessageCount == 2);

        var entry = await GetEntryAsync(orderId.ToString());

        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.State.IsConfirmed, Is.True);
        Assert.That(entry.State.MessageCount, Is.EqualTo(2));
    }

    /// <summary>
    /// Verifies that two concurrent publishes for the same correlation ID are serialised
    /// by the PostgreSQL advisory lock: the second message is processed only after the
    /// first completes, so the final message count is exactly 2.
    /// </summary>
    [Test]
    public async Task Saga_ConcurrentMessages_SameCorrelation_Serialised()
    {
        var orderId = Guid.NewGuid();

        await using var scope = _provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        // Publish two messages concurrently for the same correlation ID.
        await Task.WhenAll(
            publisher.PublishAsync(new PgSagaOrderStarted(orderId, "concurrent@pg.com")),
            publisher.PublishAsync(new PgSagaOrderConfirmed(orderId)));

        await WaitUntilAsync(async () =>
            (await GetEntryAsync(orderId.ToString()))?.State.MessageCount == 2);

        var entry = await GetEntryAsync(orderId.ToString());

        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.State.MessageCount, Is.EqualTo(2),
            "Both messages must be processed exactly once");
    }
}
