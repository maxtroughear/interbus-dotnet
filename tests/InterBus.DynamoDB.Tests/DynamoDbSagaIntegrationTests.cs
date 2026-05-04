using InterBus.DynamoDB.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.DynamoDB.Tests;

[TestFixture]
public class DynamoDbSagaIntegrationTests : DynamoDbSagaTestFixtureBase
{
    [Test]
    public async Task Saga_SingleMessage_StateIsPersisted()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<OrderSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName = TableName));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "test@example.com"));

            await TestHelpers.WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
                return repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.State.MessageCount == 1;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
            var entry = await repository.GetAsync(orderId.ToString());

            Assert.That(entry, Is.Not.Null);
            Assert.That(entry!.State.CustomerEmail, Is.EqualTo("test@example.com"));
            Assert.That(entry.State.MessageCount, Is.EqualTo(1));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Saga_MultipleMessages_StateIsAccumulated()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<OrderSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName = TableName));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "multi@example.com"));
            await publisher.PublishAsync(new OrderConfirmed(orderId));

            await TestHelpers.WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
                var entry = repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult();
                return entry?.State.MessageCount == 2 && entry.State.IsConfirmed;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderSaga, OrderSagaState>>();
            var entry = await repository.GetAsync(orderId.ToString());

            Assert.That(entry, Is.Not.Null);
            Assert.That(entry!.State.IsConfirmed, Is.True);
            Assert.That(entry.State.MessageCount, Is.EqualTo(2));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Saga_DeleteOnCancel_RemovesEntry()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<OrderWithDeletionSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName = TableName));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var repository = provider.GetRequiredService<ISagaRepository<OrderWithDeletionSaga, OrderSagaState>>();

            await publisher.PublishAsync(new OrderStarted(orderId, "del@example.com"));

            await TestHelpers.WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.State.MessageCount == 1);

            await publisher.PublishAsync(new OrderCancelled(orderId));

            await TestHelpers.WaitUntilAsync(() =>
                repository.GetAsync(orderId.ToString()).GetAwaiter().GetResult() is null);

            var entry = await repository.GetAsync(orderId.ToString());

            Assert.That(entry, Is.Null);
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Saga_Complete_DropsSubsequentMessages()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<CompletingOrderSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName = TableName));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "complete@example.com"));
            await publisher.PublishAsync(new OrderConfirmed(orderId));

            await TestHelpers.WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<CompletingOrderSaga, OrderSagaState>>();
                var entry = repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult();
                return entry?.IsCompleted == true;
            });

            var beforeCount = (await provider.GetRequiredService<ISagaRepository<CompletingOrderSaga, OrderSagaState>>()
                .GetAsync(orderId.ToString()))!.State.MessageCount;

            await publisher.PublishAsync(new OrderConfirmed(orderId));
            await Task.Delay(500);

            var entry = await provider.GetRequiredService<ISagaRepository<CompletingOrderSaga, OrderSagaState>>()
                .GetAsync(orderId.ToString());

            Assert.That(entry, Is.Not.Null);
            Assert.That(entry!.IsCompleted, Is.True);
            Assert.That(entry.State.MessageCount, Is.EqualTo(beforeCount),
                "No extra messages should be processed after completion");
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }
}
