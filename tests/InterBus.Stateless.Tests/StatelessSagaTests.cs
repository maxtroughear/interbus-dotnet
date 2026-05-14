using InterBus;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Sagas;
using InterBus.Stateless;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Stateless;
using Xunit;

namespace InterBus.Stateless.Tests;

// ---------------------------------------------------------------------------
// State machine types
// ---------------------------------------------------------------------------

public enum TrafficLightState { Red, Green, Yellow }

public enum TrafficLightTrigger { GoGreen, GoYellow, GoRed }

// ---------------------------------------------------------------------------
// Saga messages
// ---------------------------------------------------------------------------

public record TrafficLightCreated(Guid Id);

public record TurnGreen(Guid Id);

public record TurnYellow(Guid Id);

public record TurnRed(Guid Id);

// ---------------------------------------------------------------------------
// Saga state
// ---------------------------------------------------------------------------

public sealed class TrafficLightSagaState : IStateMachineSagaState<TrafficLightState>
{
    public TrafficLightState CurrentState { get; set; } = TrafficLightState.Red;
    public int TransitionCount { get; set; }
}

// ---------------------------------------------------------------------------
// Saga
// ---------------------------------------------------------------------------

public sealed class TrafficLightSaga
    : StatelessSaga<TrafficLightSagaState, TrafficLightState, TrafficLightTrigger>,
        ISagaConsumer<TrafficLightSagaState, TrafficLightCreated>,
        ISagaConsumer<TrafficLightSagaState, TurnGreen>,
        ISagaConsumer<TrafficLightSagaState, TurnYellow>,
        ISagaConsumer<TrafficLightSagaState, TurnRed>
{
    /// <summary>Exposes the protected base-class method for unit testing.</summary>
    public StateMachine<TrafficLightState, TrafficLightTrigger> ExposedCreateStateMachine(
        TrafficLightSagaState state) => CreateStateMachine(state);

    protected override void Configure(
        StateMachine<TrafficLightState, TrafficLightTrigger> machine,
        TrafficLightSagaState state)
    {
        machine.Configure(TrafficLightState.Red)
            .Permit(TrafficLightTrigger.GoGreen, TrafficLightState.Green);

        machine.Configure(TrafficLightState.Green)
            .Permit(TrafficLightTrigger.GoYellow, TrafficLightState.Yellow);

        machine.Configure(TrafficLightState.Yellow)
            .Permit(TrafficLightTrigger.GoRed, TrafficLightState.Red)
            .OnEntry(() => state.TransitionCount++);
    }

    public string GetCorrelationId(TrafficLightCreated message) => message.Id.ToString();
    public string GetCorrelationId(TurnGreen message) => message.Id.ToString();
    public string GetCorrelationId(TurnYellow message) => message.Id.ToString();
    public string GetCorrelationId(TurnRed message) => message.Id.ToString();

    public Task Consume(TrafficLightCreated message, ISagaContext<TrafficLightSagaState> context,
        CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    public async Task Consume(TurnGreen message, ISagaContext<TrafficLightSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = CreateStateMachine(context.State);
        await machine.FireAsync(TrafficLightTrigger.GoGreen);
    }

    public async Task Consume(TurnYellow message, ISagaContext<TrafficLightSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = CreateStateMachine(context.State);
        await machine.FireAsync(TrafficLightTrigger.GoYellow);
    }

    public async Task Consume(TurnRed message, ISagaContext<TrafficLightSagaState> context,
        CancellationToken cancellationToken = default)
    {
        var machine = CreateStateMachine(context.State);
        await machine.FireAsync(TrafficLightTrigger.GoRed);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

public class StatelessSagaTests
{
    private static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 5000,
        int intervalMs = 25)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }

    private static async Task<(ServiceProvider Provider, CancellationTokenSource Cts)> StartAsync(
        Action<InterBusBuilder> configureBus)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(configureBus);

        var provider = services.BuildServiceProvider();
        var cts = new CancellationTokenSource();

        foreach (var svc in provider.GetServices<IHostedService>())
            await svc.StartAsync(cts.Token);

        return (provider, cts);
    }

    private static async Task StopAsync(ServiceProvider provider, CancellationTokenSource cts)
    {
        await cts.CancelAsync();
        foreach (var svc in provider.GetServices<IHostedService>())
        {
            try { await svc.StopAsync(CancellationToken.None); }
            catch (OperationCanceledException) { }
        }

        await provider.DisposeAsync();
        cts.Dispose();
    }

    [Fact]
    public async Task StateMachine_ValidTransition_UpdatesCurrentState()
    {
        var id = Guid.NewGuid();
        var (provider, cts) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<TrafficLightSaga, TrafficLightSagaState>(s => s.UseInMemoryRepository());
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new TrafficLightCreated(id), TestContext.Current.CancellationToken);
            await publisher.PublishAsync(new TurnGreen(id), TestContext.Current.CancellationToken);

            var repo = provider.GetRequiredService<ISagaRepository<TrafficLightSaga, TrafficLightSagaState>>();
            await WaitUntilAsync(() =>
            {
                var e = repo.GetAsync(id.ToString()).GetAwaiter().GetResult();
                return e?.State.CurrentState == TrafficLightState.Green;
            });

            var entry = await repo.GetAsync(id.ToString(), TestContext.Current.CancellationToken);
            Assert.NotNull(entry);
            Assert.Equal(TrafficLightState.Green, entry.State.CurrentState);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task StateMachine_FullCycle_TrackesEachTransition()
    {
        var id = Guid.NewGuid();
        var (provider, cts) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<TrafficLightSaga, TrafficLightSagaState>(s => s.UseInMemoryRepository());
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new TrafficLightCreated(id), TestContext.Current.CancellationToken);
            await publisher.PublishAsync(new TurnGreen(id), TestContext.Current.CancellationToken);
            await publisher.PublishAsync(new TurnYellow(id), TestContext.Current.CancellationToken);
            await publisher.PublishAsync(new TurnRed(id), TestContext.Current.CancellationToken);

            var repo = provider.GetRequiredService<ISagaRepository<TrafficLightSaga, TrafficLightSagaState>>();
            await WaitUntilAsync(() =>
            {
                var e = repo.GetAsync(id.ToString()).GetAwaiter().GetResult();
                return e?.State.CurrentState == TrafficLightState.Red
                    && e.State.TransitionCount == 1;
            });

            var entry = await repo.GetAsync(id.ToString(), TestContext.Current.CancellationToken);
            Assert.NotNull(entry);
            Assert.Equal(TrafficLightState.Red, entry.State.CurrentState);
            Assert.Equal(1, entry.State.TransitionCount);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public async Task StateMachine_DifferentCorrelationIds_MaintainSeparateState()
    {
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var (provider, cts) = await StartAsync(bus =>
        {
            bus.AddInMemoryTransport();
            bus.AddSaga<TrafficLightSaga, TrafficLightSagaState>(s => s.UseInMemoryRepository());
        });

        try
        {
            using var scope = provider.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            await publisher.PublishAsync(new TrafficLightCreated(id1), TestContext.Current.CancellationToken);
            await publisher.PublishAsync(new TrafficLightCreated(id2), TestContext.Current.CancellationToken);

            // Advance id1 to Green
            await publisher.PublishAsync(new TurnGreen(id1), TestContext.Current.CancellationToken);

            var repo = provider.GetRequiredService<ISagaRepository<TrafficLightSaga, TrafficLightSagaState>>();

            await WaitUntilAsync(() =>
            {
                var e = repo.GetAsync(id1.ToString()).GetAwaiter().GetResult();
                return e?.State.CurrentState == TrafficLightState.Green;
            });

            var entry1 = await repo.GetAsync(id1.ToString(), TestContext.Current.CancellationToken);
            var entry2 = await repo.GetAsync(id2.ToString(), TestContext.Current.CancellationToken);

            Assert.NotNull(entry1);
            Assert.NotNull(entry2);
            Assert.Equal(TrafficLightState.Green, entry1.State.CurrentState);
            Assert.Equal(TrafficLightState.Red, entry2.State.CurrentState);
        }
        finally
        {
            await StopAsync(provider, cts);
        }
    }

    [Fact]
    public void CreateStateMachine_StateAccessorMutator_ReadsAndWritesThroughState()
    {
        var saga = new TrafficLightSaga();
        var state = new TrafficLightSagaState { CurrentState = TrafficLightState.Red };

        var machine = saga.ExposedCreateStateMachine(state);

        Assert.Equal(TrafficLightState.Red, machine.State);

        machine.Fire(TrafficLightTrigger.GoGreen);
        Assert.Equal(TrafficLightState.Green, machine.State);
        Assert.Equal(TrafficLightState.Green, state.CurrentState);
    }
}
