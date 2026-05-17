namespace InterBus.Stateless;

/// <summary>
/// Applied to saga state types that track the current state of a stateless state machine.
/// The <see cref="CurrentState"/> property is read and written by
/// <see cref="StatelessSaga{TState,TStateMachineState,TTrigger}.CreateStateMachine"/> so that
/// the machine state is automatically persisted with the saga state after each message dispatch.
/// </summary>
/// <typeparam name="TStateMachineState">
/// The state-machine state type (typically an <c>enum</c>).
/// </typeparam>
public interface IStateMachineSagaState<TStateMachineState>
    where TStateMachineState : notnull
{
    /// <summary>
    /// The current state of the state machine for this saga instance.
    /// Initialise this property in the saga state constructor to set the starting state.
    /// </summary>
    TStateMachineState CurrentState { get; set; }
}
