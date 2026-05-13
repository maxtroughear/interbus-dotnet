using Amazon.DynamoDBv2.Model;
using InterBus.DynamoDB.Extensions;
using InterBus.DynamoDB.Outbox;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.DynamoDB.Tests;

[TestFixture]
public class DynamoDbSagaOutboxIntegrationTests : DynamoDbSagaTestFixtureBase
{
    private string _outboxTableName = string.Empty;

    [SetUp]
    public async Task SetUpOutboxTable()
    {
        var runId = Guid.NewGuid().ToString("N");
        _outboxTableName = $"{runId}-InterBusOutboxTest";


        await DynamoDbOutboxTable.EnsureTableCreatedAsync(DynamoDb, _outboxTableName,
            new DynamoDbOutboxOptions().GsiIndexName);
    }

    [TearDown]
    public async Task TearDownOutboxTable()
    {
        await DynamoDb.DeleteTableAsync(_outboxTableName);
    }

    [Test]
    public async Task Saga_SingleMessage_StateIsPersisted()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddDynamoDbOutbox(o => { o.TableName = _outboxTableName; });
            bus.AddSaga<OrderSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName(TableName).ShareOutboxTransaction())
                    .UseDynamoDbOutbox());
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
    public async Task Saga_TransactionalPublish_StateIsPersisted()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddDynamoDbOutbox(o => { o.TableName = _outboxTableName; });
            bus.AddSaga<OrderWithPublishSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName(TableName).ShareOutboxTransaction())
                    .UseDynamoDbOutbox());
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "test@example.com"));

            await TestHelpers.WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderWithPublishSaga, OrderSagaState>>();
                return repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.State.IsConfirmed is true;
            });

            var repository = provider.GetRequiredService<ISagaRepository<OrderWithPublishSaga, OrderSagaState>>();
            var entry = await repository.GetAsync(orderId.ToString());

            Assert.That(entry, Is.Not.Null);
            Assert.That(entry!.State.CustomerEmail, Is.EqualTo("test@example.com"));
            Assert.That(entry.State.IsConfirmed, Is.True);
            Assert.That(entry.State.MessageCount, Is.EqualTo(2));
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    [Test]
    public async Task Saga_TransactionalFailure_StateNotPersisted()
    {
        var orderId = Guid.NewGuid();

        var (provider, cts) = await StartBusAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddDynamoDbOutbox(o => { o.TableName = _outboxTableName; });
            bus.AddSaga<OrderWithPublishSaga, OrderSagaState>(saga =>
                saga.UseDynamoDbRepository(o => o.TableName(TableName).ShareOutboxTransaction())
                    .UseDynamoDbOutbox());

            bus.Services.AddSingleton<FailingOutboxContext>();
            bus.Services.Replace(
                ServiceDescriptor.Singleton<IDynamoDbOutboxContext>(sp =>
                    sp.GetRequiredService<FailingOutboxContext>()));
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            await publisher.PublishAsync(new OrderStarted(orderId, "test@example.com"));

            await TestHelpers.WaitUntilAsync(() =>
            {
                var repo = provider.GetRequiredService<ISagaRepository<OrderWithPublishSaga, OrderSagaState>>();
                return repo.GetAsync(orderId.ToString()).GetAwaiter().GetResult()?.State is not null;
            }, timeoutMs: 10_000);

            var repository = provider.GetRequiredService<ISagaRepository<OrderWithPublishSaga, OrderSagaState>>();
            var outboxContext = provider.GetRequiredService<FailingOutboxContext>();
            var entry = await repository.GetAsync(orderId.ToString());

            Assert.That(outboxContext.Items, Has.Count.EqualTo(1), "Expected one outbox item to be added");
            Assert.That(entry, Is.Null);
        }
        finally
        {
            await StopBusAsync(provider, cts);
        }
    }

    private class FailingOutboxContext : IDynamoDbOutboxContext
    {
        internal List<TransactWriteItem> Items { get; } = [];

        public void AddTransactItem(TransactWriteItem item)
        {
            Items.Add(item);
        }

        public Task CommitAsync(CancellationToken cancellationToken = default)
        {
            throw new TimeoutException("Outbox commit timed out");
        }
    }
}
