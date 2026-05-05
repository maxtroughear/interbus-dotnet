using InterBus.DynamoDB.Sagas;
using InterBus.Sagas;

namespace InterBus.DynamoDB.Tests;

[TestFixture]
public class DynamoDbSagaRepositoryTests : DynamoDbSagaTestFixtureBase
{
    [Test]
    public async Task GetAsync_WhenNoEntry_ReturnsNull()
    {
        var repo = CreateRepo();
        var result = await repo.GetAsync(Guid.NewGuid().ToString());
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task SaveAndGet_RoundTripsState()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        var state = new OrderSagaState { CustomerEmail = "test@example.com", MessageCount = 3 };
        var entry = new SagaEntry<OrderSagaState>(state);

        await repo.SaveAsync(correlationId, entry);

        var loaded = await repo.GetAsync(correlationId);

        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.State.CustomerEmail, Is.EqualTo("test@example.com"));
        Assert.That(loaded.State.MessageCount, Is.EqualTo(3));
        Assert.That(loaded.IsCompleted, Is.False);
    }

    [Test]
    public async Task SaveAndGet_PreservesIsCompleted()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        var state = new OrderSagaState { MessageCount = 1 };
        var entry = new SagaEntry<OrderSagaState>(state) { IsCompleted = true };

        await repo.SaveAsync(correlationId, entry);

        var loaded = await repo.GetAsync(correlationId);

        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.IsCompleted, Is.True);
    }

    [Test]
    public async Task DeleteAsync_RemovesEntry()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        var entry = new SagaEntry<OrderSagaState>(new OrderSagaState());
        await repo.SaveAsync(correlationId, entry);

        await repo.DeleteAsync(correlationId);

        var loaded = await repo.GetAsync(correlationId);
        Assert.That(loaded, Is.Null);
    }

    [Test]
    public async Task DeleteAsync_NonExistentEntry_IsNoOp()
    {
        var repo = CreateRepo();
        Assert.DoesNotThrowAsync(async () =>
            await repo.DeleteAsync(Guid.NewGuid().ToString()));
    }

    [Test]
    public async Task DifferentSagaTypes_SameCorrelationId_AreIsolated()
    {
        var correlationId = Guid.NewGuid().ToString();

        var repoOrder = new DynamoDbSagaRepository<OrderSaga, OrderSagaState>(DynamoDb, Options);
        var repoDeletion = new DynamoDbSagaRepository<OrderWithDeletionSaga, OrderSagaState>(DynamoDb, Options);

        var orderState = new OrderSagaState { CustomerEmail = "order@example.com", MessageCount = 1 };
        var deletionSagaState = new OrderSagaState { CustomerEmail = "deletion@example.com", MessageCount = 99 };

        await repoOrder.SaveAsync(correlationId, new SagaEntry<OrderSagaState>(orderState));
        await repoDeletion.SaveAsync(correlationId, new SagaEntry<OrderSagaState>(deletionSagaState));

        var loadedOrder = await repoOrder.GetAsync(correlationId);
        var loadedDeletion = await repoDeletion.GetAsync(correlationId);

        Assert.That(loadedOrder, Is.Not.Null);
        Assert.That(loadedOrder!.State.CustomerEmail, Is.EqualTo("order@example.com"));
        Assert.That(loadedOrder.State.MessageCount, Is.EqualTo(1));

        Assert.That(loadedDeletion, Is.Not.Null);
        Assert.That(loadedDeletion!.State.CustomerEmail, Is.EqualTo("deletion@example.com"));
        Assert.That(loadedDeletion.State.MessageCount, Is.EqualTo(99));

        await repoOrder.DeleteAsync(correlationId);

        Assert.That(await repoOrder.GetAsync(correlationId), Is.Null);
        Assert.That(await repoDeletion.GetAsync(correlationId), Is.Not.Null);
    }

    [Test]
    public async Task GetAsync_ExpiredEntry_ReturnsNull()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        var pastExpiry = DateTimeOffset.UtcNow.AddSeconds(-1);
        var entry = new SagaEntry<OrderSagaState>(new OrderSagaState(), pastExpiry);
        await repo.SaveAsync(correlationId, entry);

        var loaded = await repo.GetAsync(correlationId);
        Assert.That(loaded, Is.Null, "Expired entries should be treated as absent");
    }

    [Test]
    public async Task GetAsync_FutureExpiry_ReturnsEntry()
    {
        var repo = CreateRepo();
        var correlationId = Guid.NewGuid().ToString();

        var futureExpiry = DateTimeOffset.UtcNow.AddMinutes(5);
        var entry = new SagaEntry<OrderSagaState>(new OrderSagaState { MessageCount = 7 }, futureExpiry);
        await repo.SaveAsync(correlationId, entry);

        var loaded = await repo.GetAsync(correlationId);
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.State.MessageCount, Is.EqualTo(7));
    }
}
