namespace InterBus.Stateless.Tests;

public sealed class TrafficLightSagaState : IStateMachineSagaState<TrafficLightState>
{
    public TrafficLightState CurrentState { get; set; } = TrafficLightState.Red;
    public int TransitionCount { get; set; }
}
