using Amazon.DynamoDBv2;
using InterBus.DynamoDB.Sagas;
using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.LocalStack;

namespace InterBus.DynamoDB.Tests;

public abstract class DynamoDbSagaTestFixtureBase : DynamoDbTestFixtureBase
{
    protected IAmazonDynamoDB DynamoDb = null!;
    protected DynamoDbSagaOptions<OrderSaga> Options = null!;
    protected IDynamoDbStateWriter<OrderSaga> StateWriter = null!;
    protected string TableName = string.Empty;

    [SetUp]
    public async Task SetUp()
    {
        var runId = Guid.NewGuid().ToString("N");
        TableName = $"{runId}-InterBusSagasTest";

        var awsOptions = TestHelpers.CreateAwsOptions(Container);
        var services = new ServiceCollection();

        if (awsOptions is not null)
            services.AddDefaultAWSOptions(awsOptions);

        services.AddAWSService<IAmazonDynamoDB>();
        services.AddScoped<IDynamoDbStateWriter<OrderSaga>, DynamoDbStateWriter<OrderSaga>>();

        var provider = services.BuildServiceProvider();
        DynamoDb = provider.GetRequiredService<IAmazonDynamoDB>();
        Options = new DynamoDbSagaOptions<OrderSaga> { TableName = TableName };
        StateWriter = provider.GetRequiredService<IDynamoDbStateWriter<OrderSaga>>();

        await DynamoDbSagaTable.EnsureTableCreatedAsync(DynamoDb, TableName);
    }

    [TearDown]
    public async Task TearDown()
    {
        await DynamoDb.DeleteTableAsync(TableName);
        DynamoDb.Dispose();
    }

    protected DynamoDbSagaRepository<OrderSaga, OrderSagaState> CreateRepo() =>
        new(DynamoDb, Options, StateWriter);

    protected async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartBusAsync(
        Action<InterBusBuilder> configureBus)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var awsOptions = TestHelpers.CreateAwsOptions(Container);
        if (awsOptions is not null)
            services.AddDefaultAWSOptions(awsOptions);

        services.AddAWSService<IAmazonDynamoDB>();
        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return (provider, cts);
    }

    protected static async Task StopBusAsync(ServiceProvider provider, CancellationTokenSource cts)
    {
        await cts.CancelAsync();

        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try
            {
                await svc.StopAsync(CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
            }
        }

        await provider.DisposeAsync();
        cts.Dispose();
    }
}
