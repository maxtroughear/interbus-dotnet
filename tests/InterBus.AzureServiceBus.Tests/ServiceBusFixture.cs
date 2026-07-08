using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.MsSql;
using Testcontainers.ServiceBus;

namespace InterBus.AzureServiceBus.Tests;

public abstract class ServiceBusFixture(Action<InterBusBuilder> configureBus)
{
    private INetwork _network;
    private MsSqlContainer _sqlContainer;
    protected ServiceBusContainer Container;

    protected ServiceProvider Provider;
    private CancellationTokenSource _cts;

    [OneTimeSetUp]
    public async Task OneTimeSetUpBase()
    {
        _network = new NetworkBuilder().Build();
        _sqlContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2025-latest")
            .WithPassword("Password123!")
            .WithNetwork(_network)
            .WithNetworkAliases("mssql")
            .Build();
        Container = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
            .WithMsSqlContainer(_network, _sqlContainer, "mssql", "Password123!")
            .WithAcceptLicenseAgreement(true)
            .Build();

        await _network.CreateAsync();
        await _sqlContainer.StartAsync();
        await Container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownBase()
    {
        await Container.StopAsync();
        await _sqlContainer.StopAsync();
        await Container.DisposeAsync();
        await _sqlContainer.DisposeAsync();

        await _network.DeleteAsync();
        await _network.DisposeAsync();
    }

    [SetUp]
    public async Task SetupBase()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddInterBus(bus =>
        {
            bus.AddAzureServiceBusTransport(o =>
            {
                o.ConnectionString = Container.GetConnectionString();
                o.AdminConnectionString = Container.GetHttpConnectionString();
            });
            configureBus(bus);
        });

        Provider = services.BuildServiceProvider();
        _cts = new CancellationTokenSource();

        foreach (var svc in Provider.GetServices<IHostedService>())
            await svc.StartAsync(_cts.Token);
    }

    [TearDown]
    public async Task TearDownBase()
    {
        await _cts.CancelAsync();

        foreach (var svc in Provider.GetServices<IHostedService>())
            await svc.StopAsync(CancellationToken.None);

        _cts.Dispose();
        await Provider.DisposeAsync();
    }
}
