using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using InterBus.DynamoDB.Sagas;
using InterBus.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Testcontainers.LocalStack;

namespace InterBus.DynamoDB.Tests;

public abstract class DynamoDbSagaTestFixtureBase
{
    private readonly LocalStackContainer? _container = TestHelpers.CreateLocalStackContainer();

    protected IAmazonDynamoDB DynamoDb = null!;
    protected DynamoDbSagaOptions Options = null!;
    protected string TableName = string.Empty;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        if (_container is null)
            return;

        await _container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_container is null)
            return;
        
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

    [SetUp]
    public async Task SetUp()
    {
        var runId = Guid.NewGuid().ToString("N");
        TableName = $"{runId}-InterBusSagasTest";

        var awsOptions = TestHelpers.CreateAwsOptions(_container);
        var services = new ServiceCollection();

        if (awsOptions is not null)
            services.AddDefaultAWSOptions(awsOptions);

        services.AddAWSService<IAmazonDynamoDB>();

        var provider = services.BuildServiceProvider();
        DynamoDb = provider.GetRequiredService<IAmazonDynamoDB>();
        Options = new DynamoDbSagaOptions { TableName = TableName };

        // TODO: Manage table creation as part of the library, not the test suite
        await EnsureTableExistsAsync(DynamoDb, TableName);
    }

    [TearDown]
    public async Task TearDown()
    {
        await DynamoDb.DeleteTableAsync(TableName);
        DynamoDb.Dispose();
    }

    protected DynamoDbSagaRepository<OrderSaga, OrderSagaState> CreateRepo() =>
        new(DynamoDb, Options);

    protected async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartBusAsync(
        Action<IInterBusBuilder> configureBus)
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var awsOptions = TestHelpers.CreateAwsOptions(_container);
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

    private static async Task EnsureTableExistsAsync(IAmazonDynamoDB dynamoDb, string tableName)
    {
        try
        {
            await dynamoDb.DescribeTableAsync(tableName);
        }
        catch (ResourceNotFoundException)
        {
            await dynamoDb.CreateTableAsync(new CreateTableRequest
            {
                TableName = tableName,
                KeySchema =
                [
                    new KeySchemaElement(DynamoDbSagaRepository<OrderSaga, OrderSagaState>.AttrCorrelationId,
                        KeyType.HASH),
                    new KeySchemaElement(DynamoDbSagaRepository<OrderSaga, OrderSagaState>.AttrSagaType, KeyType.RANGE),
                ],
                AttributeDefinitions =
                [
                    new AttributeDefinition(DynamoDbSagaRepository<OrderSaga, OrderSagaState>.AttrCorrelationId,
                        ScalarAttributeType.S),
                    new AttributeDefinition(DynamoDbSagaRepository<OrderSaga, OrderSagaState>.AttrSagaType,
                        ScalarAttributeType.S),
                ],
                BillingMode = BillingMode.PAY_PER_REQUEST,
            });

            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                var desc = await dynamoDb.DescribeTableAsync(tableName);
                if (desc.Table.TableStatus == TableStatus.ACTIVE)
                    break;

                await Task.Delay(200);
            }
        }
    }
}
