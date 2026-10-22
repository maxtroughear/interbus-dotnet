using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace InterBus.PostgreSQL.Tests;

[TestFixtureSource(typeof(PostgreSqlVersions), nameof(PostgreSqlVersions.Transactional))]
public class TransactionalPacingTests(string imageTag, PostgreSqlMessagePumpMode mode) : PostgreSqlFixtureBase(
    imageTag, mode, bus =>
    {
        bus.Services.AddSingleton<State>();
        bus.AddConsumer<ReleaseConsumer>();
        var factoryType = bus.Services.Last(service => service.ServiceType == typeof(IConsumerSourceFactory)).ImplementationType!;
        bus.Services.AddSingleton<IConsumerSourceFactory>(provider => new RecordingFactory(
            (IConsumerSourceFactory)ActivatorUtilities.CreateInstance(provider, factoryType), provider.GetRequiredService<State>()));
    })
{
    [Test]
    public async Task Release_ReportsFoundWork_AndDefersWaitUntilTransactionIsDisposed()
    {
        var state = Provider.GetRequiredService<State>();
        await using var scope = Provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IPublisher>()
            .PublishAsync(new OrderPlaced(Guid.NewGuid(), "release@test", 1));
        await state.Waiting.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(state.Result.FoundWork, Is.True);
        Assert.That(state.Result.IdleWait, Is.Not.Null);
        Assert.That(state.Connection!.State, Is.EqualTo(System.Data.ConnectionState.Closed));
        Assert.That(state.Attempts, Is.EqualTo(1));
    }

    private sealed class RecordingFactory(IConsumerSourceFactory inner, State state) : IConsumerSourceFactory
    {
        public IConsumerSource Create(ConsumerDescriptor descriptor) => new RecordingSource((IScopedPollingSource)inner.Create(descriptor), state);
    }
    private sealed class RecordingSource(IScopedPollingSource inner, State state) : IScopedPollingSource
    {
        public Task Ready => inner.Ready;
        public Task<Exception> Failure => inner.Failure;
        public Task InitializeAsync(CancellationToken token) => inner.InitializeAsync(token);
        public ValueTask DisposeAsync() => inner.DisposeAsync();
        public async Task<PollResult> ProcessOnceAsync(DeliveryHandler process, CancellationToken intake, CancellationToken processing)
        {
            var result = await inner.ProcessOnceAsync(process, intake, processing);
            if (!result.FoundWork) return result;
            state.Result = result;
            if (result.IdleWait is not { } wait) return result;
            return result with { IdleWait = async token =>
            {
                state.Waiting.TrySetResult();
                await state.Continue.Task.WaitAsync(token);
                await wait(token);
            } };
        }
    }
    public sealed class State
    {
        public PollResult Result;
        public NpgsqlConnection? Connection;
        public int Attempts;
        public TaskCompletionSource Waiting { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Continue { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public sealed class ReleaseConsumer(State state) : IConsumer<OrderPlaced>
    {
        public Task Consume(OrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default)
        {
            state.Connection = context.GetPostgreSqlTransaction()!.Connection;
            Interlocked.Increment(ref state.Attempts);
            ((ConsumeContext<OrderPlaced>)context).Result = DispatchResult.Release(new IOException("retry"));
            return Task.CompletedTask;
        }
    }
}
