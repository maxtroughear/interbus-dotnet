using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.MsSql;
using Testcontainers.ServiceBus;

namespace InterBus.AzureServiceBus.Tests;

public abstract class ServiceBusFixture
{
    private INetwork _network;
    private MsSqlContainer _sqlContainer;
    protected ServiceBusContainer Container;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
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
    public async Task OneTimeTearDown()
    {
        await Container.StopAsync();
        await _sqlContainer.StopAsync();
        await Container.DisposeAsync();
        await _sqlContainer.DisposeAsync();

        await _network.DeleteAsync();
        await _network.DisposeAsync();
    }
}
