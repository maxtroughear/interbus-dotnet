using Amazon.SimpleNotificationService;
using Amazon.SQS;
using InterBus.AmazonSqs.Extensions;
using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

public abstract class LocalstackTestFixtureBase(Action<InterBusBuilder> configureBus)
{
    protected readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();
    protected ServiceProvider Provider;
    protected string QueuePrefix = null!;
    protected string TopicPrefix = null!;

    private CancellationTokenSource _cts;
    private string _runId = null!;
    
    [OneTimeSetUp]
    public async Task OneTimeSetUpBase()
    {
        if (Container is not null)
            await Container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownBase()
    {
        if (Container is not null)
        {
            await Container.StopAsync();
            await Container.DisposeAsync();
        }
    }

    [SetUp]
    public async Task SetupBase()
    {
        _runId = Guid.NewGuid().ToString("N")[..8];
        QueuePrefix = $"q-{_runId}-";
        TopicPrefix = $"t-{_runId}-";
        _cts = new CancellationTokenSource();
        
        var services = new ServiceCollection();
        services.AddLogging();

        var awsOptions = TestHelpers.CreateAwsOptions(Container);

        if (awsOptions is not null)
            services.AddDefaultAWSOptions(awsOptions);

        services.AddAWSService<IAmazonSimpleNotificationService>();
        services.AddAWSService<IAmazonSQS>();
        services.AddAWSService<Amazon.SecurityToken.IAmazonSecurityTokenService>();

        services.AddInterBus(bus =>
        {
            bus.AddAmazonSqsTransport(o =>
            {
                o.QueuePrefix = QueuePrefix;
                o.TopicPrefix = TopicPrefix;
            });
            configureBus(bus);
        });

        Provider = services.BuildServiceProvider();

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

        // Clean up AWS resources created during this test
        var sqsClient = Provider.GetRequiredService<IAmazonSQS>();
        var snsClient = Provider.GetRequiredService<IAmazonSimpleNotificationService>();
        var logger = Provider.GetRequiredService<ILogger<LocalstackTestFixtureBase>>();

        await TestHelpers.CleanupResourcesByPrefixAsync(sqsClient, snsClient, QueuePrefix, TopicPrefix, logger);

        await Provider.DisposeAsync();
    }
}
