using InterBus.Generated;
using InterBus.Extensions;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.PostgreSql;
using System.Collections.Concurrent;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.Transactional))]
public class TransactionalSettlementFaultTests(string imageTag, PostgreSqlMessagePumpMode mode)
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder(imageTag).Build();
    [OneTimeSetUp] public Task StartContainer() => _container.StartAsync();
    [OneTimeTearDown] public ValueTask DisposeContainer() => _container.DisposeAsync();

    [TestCase("ack", false)]
    [TestCase("commit", false)]
    [TestCase("commit", true)]
    [TestCase("disconnect", false)]
    [TestCase("ambiguous-commit", false)]
    [TestCase("ambiguous-commit", true)]
    public async Task SettlementFault_RecoversWithoutLeakingTransactionsOrAcknowledgingFailedWork(string fault, bool pumpCircuit)
    {
        var schema = "fault_" + Guid.NewGuid().ToString("N")[..10];
        var applicationName = schema + "_worker";
        var directSettings = new NpgsqlConnectionStringBuilder(_container.GetConnectionString());
        await using var admin = NpgsqlDataSource.Create(directSettings.ConnectionString);
        await using var proxy = new CommitResponseProxy(directSettings.Host!, directSettings.Port);
        var connectionSettings = new NpgsqlConnectionStringBuilder(directSettings.ConnectionString)
        {
            Host = "127.0.0.1", Port = proxy.Port, SslMode = SslMode.Disable,
            GssEncryptionMode = GssEncryptionMode.Disable, ApplicationName = applicationName,
            MaxPoolSize = 8, Timeout = 5, CommandTimeout = 5
        };
        await using var dataSource = NpgsqlDataSource.Create(connectionSettings.ConnectionString);
        var state = new State(schema, fault);
        var log = new CaptureLog();
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.AddProvider(log);
        builder.Services.AddSingleton(state);
        builder.Services.AddInterBus(bus =>
        {
            bus.AddPostgreSqlTransport(dataSource, options =>
            {
                options.MessagePumpMode = mode;
                options.SchemaName = schema;
                options.FallbackPollInterval = TimeSpan.FromMilliseconds(100);
                options.ConnectionRetry.RetryCount = 2;
                options.ConnectionRetry.DelayProvider = _ => TimeSpan.FromMilliseconds(50);
            });
            bus.AddConsumer<FaultConsumer>(consumer =>
            {
                if (pumpCircuit) consumer.UsePumpCircuitBreaker(options =>
                {
                    options.Transport(policy =>
                    {
                        policy.MinimumThroughput = 1;
                        policy.FailureThreshold = 0.000001;
                        policy.BreakDuration = TimeSpan.FromMilliseconds(100);
                    });
                    options.ObserveSettlementFailures = true;
                    options.ReopenJitter = TimeSpan.Zero;
                });
            });
        });
        var host = builder.Build();
        try
        {
            using var startup = CancellationTokenSource.CreateLinkedTokenSource(TestContext.CurrentContext.CancellationToken);
            startup.CancelAfter(TimeSpan.FromSeconds(15));
            await host.StartAsync(startup.Token);
            await using (var setup = admin.CreateCommand($"CREATE TABLE {schema}.ledger (message_id uuid PRIMARY KEY, transaction_id bigint NOT NULL)"))
                await setup.ExecuteNonQueryAsync();
            if (fault is "ack" or "commit") await InstallFaultAsync(admin, schema, fault == "commit");
            if (fault == "ambiguous-commit") proxy.DropNextCommitResponse();
            var target = new OrderPlaced(Guid.NewGuid(), "fault@test", 1);
            await PublishAsync(host.Services, target);
            if (fault == "disconnect")
            {
                var backend = await state.Claimed.Task.WaitAsync(TimeSpan.FromSeconds(10));
                try
                {
                    await using var terminate = admin.CreateCommand("SELECT pg_terminate_backend($1)");
                    terminate.Parameters.AddWithValue(backend);
                    Assert.That(await terminate.ExecuteScalarAsync(), Is.True);
                }
                finally { state.Resume.TrySetResult(); }
            }
            if (fault == "ambiguous-commit") await proxy.DroppedCommit.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await WaitUntilAsync(async () => await CountAsync(admin, $"SELECT COUNT(*) FROM {schema}.ledger") == 1 &&
                await CountAsync(admin, $"SELECT COUNT(*) FROM {schema}.messages") == 0 &&
                log.Messages.Any(message => message.Contains("settlement failed")),
                timeoutMs: 15_000, cancellationToken: TestContext.CurrentContext.CancellationToken);
            Assert.That(state.Attempts[target.OrderId], Is.EqualTo(fault == "ambiguous-commit" ? 1 : 2));
            Assert.That(log.Messages.Any(message => message.Contains("settlement failed")), Is.True);
            if (fault == "ambiguous-commit")
                Assert.That(log.Messages.Any(message => message.Contains("acknowledged")), Is.False);

            if (pumpCircuit && fault == "ambiguous-commit")
                Assert.That(host.Services.GetRequiredService<InterBus.MessagePumps.PumpController>().Pumps.Single()
                    .Circuits.Single(policy => policy.Policy == "Transport").State, Is.Not.EqualTo("Closed"));

            var sentinel = new OrderPlaced(Guid.NewGuid(), "after-fault@test", 2);
            await PublishAsync(host.Services, sentinel);
            await WaitUntilAsync(async () => await CountAsync(admin, $"SELECT COUNT(*) FROM {schema}.ledger") == 2 &&
                await CountAsync(admin, $"SELECT COUNT(*) FROM {schema}.messages") == 0,
                timeoutMs: 15_000, cancellationToken: TestContext.CurrentContext.CancellationToken);
            Assert.That(state.Attempts[sentinel.OrderId], Is.EqualTo(1));
            Assert.That(await CountAsync(admin, $"SELECT COUNT(*) FROM {schema}.message_payloads"), Is.Zero);
            Assert.That(await CountAsync(admin, $"SELECT COUNT(*) FROM {schema}.dead_letter_messages"), Is.Zero);
            Assert.That(state.TransactionIds.Distinct().Count(), Is.EqualTo(state.TransactionIds.Count));
            Assert.That(state.Contexts.All(context => context.GetPostgreSqlTransaction() is null), Is.True);
            using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await host.StopAsync(shutdown.Token);
        }
        finally
        {
            state.Resume.TrySetResult();
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            try { await host.StopAsync(cleanup.Token); }
            finally { await ((IAsyncDisposable)host).DisposeAsync(); }
        }
        await dataSource.DisposeAsync();
        await WaitUntilAsync(async () =>
        {
            await using var connections = admin.CreateCommand("SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = $1");
            connections.Parameters.AddWithValue(applicationName);
            return (long)(await connections.ExecuteScalarAsync())! == 0;
        }, cancellationToken: TestContext.CurrentContext.CancellationToken);
        Assert.That(proxy.Errors, Is.Empty);
    }

    private static async Task InstallFaultAsync(NpgsqlDataSource dataSource, string schema, bool atCommit)
    {
        var trigger = atCommit
            ? $"CREATE CONSTRAINT TRIGGER fail_settlement AFTER DELETE ON {schema}.messages DEFERRABLE INITIALLY DEFERRED"
            : $"CREATE TRIGGER fail_settlement BEFORE DELETE ON {schema}.messages";
        await using var setup = dataSource.CreateCommand($"""
            CREATE SEQUENCE {schema}.fault_attempt;
            CREATE FUNCTION {schema}.fail_once() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF nextval('{schema}.fault_attempt') = 1 THEN
                    RAISE EXCEPTION 'injected settlement failure';
                END IF;
                RETURN OLD;
            END $$;
            {trigger} FOR EACH ROW EXECUTE FUNCTION {schema}.fail_once();
            """);
        await setup.ExecuteNonQueryAsync();
    }
    private static async Task<long> CountAsync(NpgsqlDataSource source, string sql)
    {
        await using var command = source.CreateCommand(sql);
        return (long)(await command.ExecuteScalarAsync())!;
    }
    private static async Task PublishAsync(IServiceProvider provider, OrderPlaced message)
    {
        await using var scope = provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IPublisher>().PublishAsync(message);
    }
    public sealed class State(string schema, string fault)
    {
        public string Schema => schema;
        public string Fault => fault;
        public ConcurrentDictionary<Guid, int> Attempts { get; } = new();
        public ConcurrentBag<long> TransactionIds { get; } = [];
        public ConcurrentBag<IConsumeContext> Contexts { get; } = [];
        public TaskCompletionSource<int> Claimed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class FaultConsumer(State state) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            var transaction = context.GetPostgreSqlTransaction() ?? throw new InvalidOperationException("Missing transaction");
            var attempt = state.Attempts.AddOrUpdate(message.OrderId, 1, (_, count) => count + 1);
            await using var command = transaction.Connection!.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT txid_current()";
            state.TransactionIds.Add((long)(await command.ExecuteScalarAsync(cancellationToken))!);
            command.CommandText = $"INSERT INTO {state.Schema}.ledger VALUES ($1, txid_current())";
            command.Parameters.AddWithValue(message.OrderId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            state.Contexts.Add(context);
            if (state.Fault == "disconnect" && attempt == 1 && message.Total == 1)
            {
                state.Claimed.TrySetResult(transaction.Connection.ProcessID);
                await state.Resume.Task.WaitAsync(cancellationToken);
            }
            if (!ReferenceEquals(transaction, context.GetPostgreSqlTransaction()))
                throw new InvalidOperationException("Ambient transaction changed during dispatch");
        }
    }
    private sealed class CaptureLog : ILoggerProvider, ILogger
    {
        public ConcurrentQueue<string> Messages { get; } = new();
        public ILogger CreateLogger(string name) => this;
        public bool IsEnabled(LogLevel level) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Messages.Enqueue(formatter(state, exception));
        public void Dispose() { }
    }
}
