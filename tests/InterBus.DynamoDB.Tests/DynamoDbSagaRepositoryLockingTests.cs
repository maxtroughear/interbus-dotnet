using InterBus.DynamoDB.Sagas;

namespace InterBus.DynamoDB.Tests;

[TestFixture]
public class DynamoDbSagaRepositoryLockingTests : DynamoDbSagaTestFixtureBase
{
    [Test]
    public async Task AcquireLock_ThenRelease_AllowsReAcquisition()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        await using (await repo.AcquireLockAsync(correlationId))
        {
            var shortOpts = new DynamoDbSagaOptions
            {
                TableName = TableName,
                LockLeaseDuration = TimeSpan.FromSeconds(30),
                LockRetryInterval = TimeSpan.FromMilliseconds(100),
                LockAcquireTimeout = TimeSpan.FromMilliseconds(300),
            };
            var repo2 = new DynamoDbSagaRepository<OrderSaga, OrderSagaState>(DynamoDb, shortOpts);
            Assert.ThrowsAsync<TimeoutException>(async () =>
                await repo2.AcquireLockAsync(correlationId));
        }

        await using var _ = await repo.AcquireLockAsync(correlationId);
    }

    [Test]
    public async Task AcquireLock_AfterLeaseExpiry_ClaimsAbandonedLock()
    {
        var shortLease = new DynamoDbSagaOptions
        {
            TableName = TableName,
            LockLeaseDuration = TimeSpan.FromSeconds(1),
            LockRetryInterval = TimeSpan.FromMilliseconds(100),
            LockAcquireTimeout = TimeSpan.FromSeconds(10),
        };
        var repo = new DynamoDbSagaRepository<OrderSaga, OrderSagaState>(DynamoDb, shortLease);
        var correlationId = Guid.NewGuid().ToString();

        _ = await repo.AcquireLockAsync(correlationId);
        await Task.Delay(TimeSpan.FromSeconds(2));

        await using var lock2 = await repo.AcquireLockAsync(correlationId);
        Assert.That(lock2, Is.Not.Null);
    }
}
