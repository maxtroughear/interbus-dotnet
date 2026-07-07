using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.MsSql;
using Testcontainers.ServiceBus;

namespace InterBus.AzureServiceBus.Tests;

public class ServiceBusFixture
{
    protected INetwork _network;
    protected MsSqlContainer _sqlContainer;
    protected ServiceBusContainer _container;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _network = new NetworkBuilder().Build();
        _sqlContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2025-latest")
            .WithPassword("Password123!")
            .WithNetwork(_network)
            .WithNetworkAliases("mssql")
            .Build();
        _container = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
            .WithMsSqlContainer(_network, _sqlContainer, "mssql", "Password123!")
            .WithAcceptLicenseAgreement(true)
            .Build();

        await _network.CreateAsync();
        await _sqlContainer.StartAsync();
        await _container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _container.StopAsync();
        await _sqlContainer.StopAsync();
        await _container.DisposeAsync();
        await _sqlContainer.DisposeAsync();

        await _network.DeleteAsync();
        await _network.DisposeAsync();
    }
}
