using InterBus.Extensions;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.PostgreSql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.All))]
public abstract class PostgreSqlFixtureBase(
    string imageTag,
    PostgreSqlMessagePumpMode pumpMode,
    Action<InterBusBuilder> configureBus,
    Action<PostgreSqlTransportOptions>? configurePostgreSql = null)
{
    protected readonly PostgreSqlMessagePumpMode PumpMode = pumpMode;
    
    protected readonly PostgreSqlContainer Container = new PostgreSqlBuilder(imageTag).Build();

    protected ServiceProvider Provider;
    private CancellationTokenSource _cts;

    [OneTimeSetUp]
    public Task OneTimeSetUp() => Container.StartAsync();

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await Container.StopAsync();
        await Container.DisposeAsync();
    }

    [SetUp]
    public async Task SetupBase()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddInterBus(bus =>
        {
            bus.AddPostgreSqlTransport(o =>
            {
                o.ConnectionString = Container.GetConnectionString();
                o.ConnectionRetry.RetryCount = 5;
                o.FallbackPollInterval = TimeSpan.FromMilliseconds(200);
                o.MessagePumpMode = PumpMode;
                configurePostgreSql?.Invoke(o);
            });
            configureBus(bus);
        });

        Provider = services.BuildServiceProvider();
        _cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.CurrentContext.CancellationToken);

        foreach (var svc in Provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }

    [TearDown]
    public async Task TearDownBase()
    {
        await _cts.CancelAsync();

        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        foreach (var svc in Provider.GetServices<IHostedService>())
            await svc.StopAsync(deadline.Token).WaitAsync(deadline.Token);

        _cts.Dispose();
        await Provider.DisposeAsync();
    }
}
