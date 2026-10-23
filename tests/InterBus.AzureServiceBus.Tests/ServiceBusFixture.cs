using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
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
    protected static bool UseRealAzure => string.Equals(
        Environment.GetEnvironmentVariable("INTERBUS_USE_REAL_AZURE"), "true", StringComparison.OrdinalIgnoreCase);
    protected string EntityPrefix { get; private set; } = string.Empty;
    private readonly AzureCliCredential _credential = new();
    private string Namespace => Environment.GetEnvironmentVariable("INTERBUS_AZURE_SERVICEBUS_NAMESPACE")
        ?? throw new InvalidOperationException("INTERBUS_AZURE_SERVICEBUS_NAMESPACE is required for real Azure tests");

    protected ServiceBusAdministrationClient CreateAdminClient() => UseRealAzure
        ? new ServiceBusAdministrationClient(Namespace, _credential)
        : new ServiceBusAdministrationClient(Container.GetHttpConnectionString());

    protected ServiceBusClient CreateClient() => UseRealAzure
        ? new ServiceBusClient(Namespace, _credential)
        : new ServiceBusClient(Container.GetConnectionString());

    private INetwork _network;
    private MsSqlContainer _sqlContainer;
    protected ServiceBusContainer Container;

    protected ServiceProvider Provider;
    private CancellationTokenSource _cts;

    [OneTimeSetUp]
    public async Task OneTimeSetUpBase()
    {
        if (UseRealAzure)
        {
            _ = Namespace;
            return;
        }
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
        if (UseRealAzure) return;
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
        EntityPrefix = UseRealAzure ? $"int-test-{Guid.NewGuid():N}-" : string.Empty;
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddInterBus(bus =>
        {
            bus.AddAzureServiceBusTransport(o =>
            {
                if (UseRealAzure)
                {
                    o.FullyQualifiedNamespace = Namespace;
                    o.Credential = _credential;
                    o.QueuePrefix = EntityPrefix;
                    o.TopicPrefix = EntityPrefix;
                }
                else
                {
                    o.ConnectionString = Container.GetConnectionString();
                    o.AdminConnectionString = Container.GetHttpConnectionString();
                }
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
        if (UseRealAzure)
        {
            var admin = CreateAdminClient();
            await foreach (var topic in admin.GetTopicsAsync())
                if (topic.Name.StartsWith(EntityPrefix, StringComparison.Ordinal))
                    await admin.DeleteTopicAsync(topic.Name);
            await foreach (var queue in admin.GetQueuesAsync())
                if (queue.Name.StartsWith(EntityPrefix, StringComparison.Ordinal))
                    await admin.DeleteQueueAsync(queue.Name);
        }
    }
}
