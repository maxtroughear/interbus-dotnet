using System.Reflection;
using System.Text.Json;
using InterBus.Consuming;
using InterBus.Generated;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.Transactional))]
public class TransactionalNotificationTests(string imageTag, PostgreSqlMessagePumpMode mode) : PostgreSqlFixtureBase(
    imageTag, mode, bus =>
    {
        bus.Services.AddSingleton<Gate>();
        bus.AddConsumer<BlockingConsumer>(consumer => consumer.WithConcurrency(3));
        var factoryType = bus.Services.Last(service => service.ServiceType == typeof(IConsumerSourceFactory)).ImplementationType!;
        bus.Services.AddSingleton<IConsumerSourceFactory>(provider => new RecordingFactory(
            (IConsumerSourceFactory)ActivatorUtilities.CreateInstance(provider, factoryType), provider.GetRequiredService<Gate>()));
    }, options => options.FallbackPollInterval = TimeSpan.FromSeconds(30))
{
    [Test]
    public async Task OneCoalescedNotification_WakesAllIdleTransactionWorkers()
    {
        var gate = Provider.GetRequiredService<Gate>();
        var dataSource = Provider.GetRequiredService<NpgsqlDataSource>();
        var descriptor = Provider.GetRequiredService<InterBusOptions>().Consumers.Single();
        await WaitUntilAsync(() => gate.Source is not null && (int)gate.Source.GetType().BaseType!
            .GetProperty("WaitingForWorkCount", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(gate.Source)! == 3,
            timeoutMs: 10_000, cancellationToken: TestContext.CurrentContext.CancellationToken);
        await WaitUntilAsync(async () =>
        {
            await using var command = dataSource.CreateCommand("SELECT COUNT(*) FROM pg_stat_activity WHERE query LIKE 'LISTEN%' AND datname = current_database()");
            return (long)(await command.ExecuteScalarAsync())! > 0;
        }, cancellationToken: TestContext.CurrentContext.CancellationToken);
        try
        {
            await using var connection = await dataSource.OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            var serializer = Provider.GetRequiredService<IMessageSerializer>();
            for (var i = 0; i < 3; i++)
            {
                await using var send = connection.CreateCommand();
                send.Transaction = transaction;
                send.CommandText = "SELECT interbus.send_to_queue($1, $2, $3)";
                send.Parameters.AddWithValue(descriptor.QueueName);
                send.Parameters.AddWithValue(serializer.Serialize(new OrderPlaced(Guid.NewGuid(), "burst", i)));
                send.Parameters.AddWithValue(JsonSerializer.Serialize(new Dictionary<string, string>
                { [InterBusHeaders.MessageType] = RoutingConventions.MessageTypeString(typeof(OrderPlaced)) }));
                await send.ExecuteNonQueryAsync();
            }
            await transaction.CommitAsync();
            await gate.AllEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(gate.Active, Is.EqualTo(3));
        }
        finally { gate.Release.TrySetResult(); }
        await gate.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private sealed class RecordingFactory(IConsumerSourceFactory inner, Gate gate) : IConsumerSourceFactory
    {
        public IConsumerSource Create(ConsumerDescriptor descriptor) => gate.Source = inner.Create(descriptor);
    }
    public sealed class Gate
    {
        public IConsumerSource? Source;
        public int Active, Finished;
        public TaskCompletionSource AllEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class BlockingConsumer(Gate gate) : IConsumer<OrderPlaced>
    {
        public async Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref gate.Active) == 3) gate.AllEntered.TrySetResult();
            try { await gate.Release.Task.WaitAsync(cancellationToken); }
            finally
            {
                Interlocked.Decrement(ref gate.Active);
                if (Interlocked.Increment(ref gate.Finished) == 3) gate.Completed.TrySetResult();
            }
        }
    }
}
