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
            var shortOpts = new DynamoDbSagaOptions<OrderSaga>
            {
                TableName = TableName,
                LockLeaseDuration = TimeSpan.FromSeconds(30),
                LockRetryInterval = TimeSpan.FromMilliseconds(100),
                LockAcquireTimeout = TimeSpan.FromMilliseconds(300),
            };
            var repo2 = new DynamoDbSagaRepository<OrderSaga, OrderSagaState>(DynamoDb, shortOpts, StateWriter);
            Assert.ThrowsAsync<TimeoutException>(async () =>
                await repo2.AcquireLockAsync(correlationId));
        }

        await using var _ = await repo.AcquireLockAsync(correlationId);
    }

    [Test]
    public async Task AcquireLock_AfterLeaseExpiry_ClaimsAbandonedLock()
    {
        var shortLease = new DynamoDbSagaOptions<OrderSaga>
        {
            TableName = TableName,
            LockLeaseDuration = TimeSpan.FromSeconds(2),
            LockRetryInterval = TimeSpan.FromMilliseconds(100),
            LockAcquireTimeout = TimeSpan.FromSeconds(10),
        };
        var repo = new DynamoDbSagaRepository<OrderSaga, OrderSagaState>(DynamoDb, shortLease, StateWriter);
        var correlationId = Guid.NewGuid().ToString();

        var lock1 = await repo.AcquireLockAsync(correlationId);
        await Task.Delay(TimeSpan.FromSeconds(2));

        await lock1.DisposeAsync(); // Abandon the lock
        
        await using var lock2 = await repo.AcquireLockAsync(correlationId);
        Assert.That(lock2, Is.Not.Null);
    }

    [Test]
    public async Task AcquireLock_WithAutoRenewal_LockDoesNotExpireWhileHeld()
    {
        var shortLease = new DynamoDbSagaOptions<OrderSaga>
        {
            TableName = TableName,
            LockLeaseDuration = TimeSpan.FromSeconds(2),
            LockRetryInterval = TimeSpan.FromMilliseconds(100),
            LockAcquireTimeout = TimeSpan.FromMilliseconds(300),
        };
        var repo = new DynamoDbSagaRepository<OrderSaga, OrderSagaState>(DynamoDb, shortLease, StateWriter);
        var repo2 = new DynamoDbSagaRepository<OrderSaga, OrderSagaState>(DynamoDb, shortLease, StateWriter);
        var correlationId = Guid.NewGuid().ToString();

        await using (await repo.AcquireLockAsync(correlationId))
        {
            // Wait longer than the lease duration; auto-renewal should keep the lock alive.
            await Task.Delay(TimeSpan.FromSeconds(4));

            // A second repo instance must NOT be able to steal the lock.
            Assert.ThrowsAsync<TimeoutException>(async () =>
                await repo2.AcquireLockAsync(correlationId));
        }
    }
}
