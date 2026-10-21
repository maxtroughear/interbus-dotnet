using InterBus.Sagas;
using Stateless;

namespace InterBus.Stateless.Tests;

public sealed class TrafficLightSaga : StatelessSaga<TrafficLightSagaState, TrafficLightState, TrafficLightTrigger>, ISagaConsumer<TrafficLightSagaState, TrafficLightCreated>, ISagaConsumer<TrafficLightSagaState, TurnGreen>, ISagaConsumer<TrafficLightSagaState, TurnYellow>, ISagaConsumer<TrafficLightSagaState, TurnRed>
{
    protected override void Configure(StateMachine<TrafficLightState, TrafficLightTrigger> machine, ISagaContext<TrafficLightSagaState> context)
    {
        machine.Configure(TrafficLightState.Red).Permit(TrafficLightTrigger.GoGreen, TrafficLightState.Green);
        machine.Configure(TrafficLightState.Green).Permit(TrafficLightTrigger.GoYellow, TrafficLightState.Yellow);
        machine.Configure(TrafficLightState.Yellow).Permit(TrafficLightTrigger.GoRed, TrafficLightState.Red).OnEntry(() => context.State.TransitionCount++);
    }

    public string GetCorrelationId(TrafficLightCreated message) => message.Id.ToString();
    public string GetCorrelationId(TurnGreen message) => message.Id.ToString();
    public string GetCorrelationId(TurnYellow message) => message.Id.ToString();
    public string GetCorrelationId(TurnRed message) => message.Id.ToString();
    public Task Consume(TrafficLightCreated message, ISagaContext<TrafficLightSagaState> context, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public async Task Consume(TurnGreen message, ISagaContext<TrafficLightSagaState> context, CancellationToken cancellationToken = default)
    {
        var machine = await CreateStateMachineAsync(context);
        await machine.FireAsync(TrafficLightTrigger.GoGreen);
    }

    public async Task Consume(TurnYellow message, ISagaContext<TrafficLightSagaState> context, CancellationToken cancellationToken = default)
    {
        var machine = await CreateStateMachineAsync(context);
        await machine.FireAsync(TrafficLightTrigger.GoYellow);
    }

    public async Task Consume(TurnRed message, ISagaContext<TrafficLightSagaState> context, CancellationToken cancellationToken = default)
    {
        var machine = await CreateStateMachineAsync(context);
        await machine.FireAsync(TrafficLightTrigger.GoRed);
    }
}
