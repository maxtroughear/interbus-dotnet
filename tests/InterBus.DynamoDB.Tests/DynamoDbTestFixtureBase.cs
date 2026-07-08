using Testcontainers.LocalStack;

namespace InterBus.DynamoDB.Tests;

public abstract class DynamoDbTestFixtureBase
{
    protected readonly LocalStackContainer? Container = TestHelpers.CreateLocalStackContainer();
    
    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        if (Container is null)
            return;

        await Container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (Container is null)
            return;

        await Container.StopAsync();
        await Container.DisposeAsync();
    }
}
