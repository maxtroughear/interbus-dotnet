using InterBus.EntityFrameworkCore.Extensions;
using InterBus.EntityFrameworkCore.Sagas;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.MsSql;

namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

// ---------------------------------------------------------------------------
// Saga messages / state / sagas for SQL Server integration tests
// ---------------------------------------------------------------------------

public record SqlSagaOrderStarted(Guid OrderId, string CustomerEmail);
public record SqlSagaOrderConfirmed(Guid OrderId);

public sealed class SqlSagaOrderState
{
    public string? CustomerEmail { get; set; }
    public bool IsConfirmed { get; set; }
    public int MessageCount { get; set; }
}

public sealed class SqlSagaOrderSaga :
    ISagaConsumer<SqlSagaOrderState, SqlSagaOrderStarted>,
    ISagaConsumer<SqlSagaOrderState, SqlSagaOrderConfirmed>
{
    public string GetCorrelationId(SqlSagaOrderStarted message) => message.OrderId.ToString();
    public string GetCorrelationId(SqlSagaOrderConfirmed message) => message.OrderId.ToString();

    public Task Consume(SqlSagaOrderStarted message, ISagaContext<SqlSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.CustomerEmail = message.CustomerEmail;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }

    public Task Consume(SqlSagaOrderConfirmed message, ISagaContext<SqlSagaOrderState> context,
        CancellationToken cancellationToken = default)
    {
        context.State.IsConfirmed = true;
        context.State.MessageCount++;
        return Task.CompletedTask;
    }
}

/// <summary>
/// Integration tests for the EF Core saga repository using a real SQL Server database.
/// Validates that saga state is persisted correctly and that the
/// <see cref="SqlServerSagaLockProvider{TDbContext}"/> correctly serialises concurrent
/// access to the same saga instance.
/// </summary>
[TestFixtureSource(typeof(SqlServerVersions), nameof(SqlServerVersions.All))]
public class SagaSqlServerIntegrationTests(string imageTag)
{
    private readonly MsSqlContainer _container = new MsSqlBuilder(imageTag).Build();
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
            o.UseSqlServer(_container.GetConnectionString()));

        services.AddInterBus(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<SqlSagaOrderSaga, SqlSagaOrderState>(saga =>
                saga.UseEfCoreRepository<SqlSagaOrderSaga, SqlSagaOrderState, SagaTestDbContext>(
                    repo => repo.UseSqlServer()));
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
            try { await svc.StopAsync(CancellationToken.None); }
            catch (OperationCanceledException) { }
        }

        await _provider.DisposeAsync();
        _cts.Dispose();
    }

    private async Task<SagaEntry<SqlSagaOrderState>?> GetEntryAsync(string correlationId)
    {
        await using var scope = _provider.CreateAsyncScope();
        var repo = scope.ServiceProvider
            .GetRequiredService<ISagaRepository<SqlSagaOrderSaga, SqlSagaOrderState>>();
        return await repo.GetAsync(correlationId);
    }

    private static async Task WaitUntilAsync(
        Func<Task<bool>> condition,
        int timeoutMs = 10_000,
        int intervalMs = 50)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!await condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

    [Test]
    public async Task Saga_SingleMessage_StateIsPersisted()
    {
        var orderId = Guid.NewGuid();

        await using var scope = _provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
        await publisher.PublishAsync(new SqlSagaOrderStarted(orderId, "sql@test.com"));

        await WaitUntilAsync(async () =>
            (await GetEntryAsync(orderId.ToString()))?.State.MessageCount == 1);

        var entry = await GetEntryAsync(orderId.ToString());

        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.State.CustomerEmail, Is.EqualTo("sql@test.com"));
        Assert.That(entry.State.MessageCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Saga_MultipleMessages_StateAccumulates()
    {
        var orderId = Guid.NewGuid();

        await using var scope = _provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        await publisher.PublishAsync(new SqlSagaOrderStarted(orderId, "order@sql.com"));
        await publisher.PublishAsync(new SqlSagaOrderConfirmed(orderId));

        await WaitUntilAsync(async () =>
            (await GetEntryAsync(orderId.ToString()))?.State.MessageCount == 2);

        var entry = await GetEntryAsync(orderId.ToString());

        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.State.IsConfirmed, Is.True);
        Assert.That(entry.State.MessageCount, Is.EqualTo(2));
    }

    /// <summary>
    /// Verifies that concurrent publishes for the same correlation ID are serialised
    /// by the SQL Server application lock: both messages must be processed exactly once.
    /// </summary>
    [Test]
    public async Task Saga_ConcurrentMessages_SameCorrelation_Serialised()
    {
        var orderId = Guid.NewGuid();

        await using var scope = _provider.CreateAsyncScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

        await Task.WhenAll(
            publisher.PublishAsync(new SqlSagaOrderStarted(orderId, "concurrent@sql.com")),
            publisher.PublishAsync(new SqlSagaOrderConfirmed(orderId)));

        await WaitUntilAsync(async () =>
            (await GetEntryAsync(orderId.ToString()))?.State.MessageCount == 2);

        var entry = await GetEntryAsync(orderId.ToString());

        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.State.MessageCount, Is.EqualTo(2),
            "Both messages must be processed exactly once");
    }
}
