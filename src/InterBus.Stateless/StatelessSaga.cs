using global::Stateless;
using InterBus.Sagas;

namespace InterBus.Stateless;

/// <summary>
/// Abstract base class for sagas that model their workflow as a
/// <a href="https://github.com/dotnet-state-machine/stateless">stateless</a> state machine.
/// <para>
/// Derive from this class and implement <see cref="ISagaConsumer{TState,TMessage}"/> for each
/// message type the saga handles. In each handler, call <see cref="CreateStateMachine"/> to obtain
/// a <see cref="StateMachine{TState,TTrigger}"/> bound to the current saga state, then fire the
/// appropriate trigger.
/// </para>
/// <para>
/// Override <see cref="Configure"/> to declare states, permitted transitions, and
/// entry/exit actions. <see cref="Configure"/> is called every time
/// <see cref="CreateStateMachine"/> is invoked, so closures in <see cref="Configure"/> may
/// safely capture <c>this</c> and access saga dependencies.
/// </para>
/// </summary>
/// <typeparam name="TState">
/// The saga state type. Must implement <see cref="IStateMachineSagaState{TStateMachineState}"/>
/// so that the state machine's current state is persisted with the saga instance.
/// </typeparam>
/// <typeparam name="TStateMachineState">The state-machine state type (typically an <c>enum</c>).</typeparam>
/// <typeparam name="TTrigger">The state-machine trigger type (typically an <c>enum</c>).</typeparam>
public abstract class StatelessSaga<TState, TStateMachineState, TTrigger>
    where TState : class, IStateMachineSagaState<TStateMachineState>, new()
    where TStateMachineState : notnull
    where TTrigger : notnull
{
    /// <summary>
    /// Configures the state machine's states, permitted transitions, and entry/exit actions.
    /// Called each time <see cref="CreateStateMachine"/> is invoked.
    /// </summary>
    /// <param name="machine">The state machine to configure.</param>
    /// <param name="state">The current saga state, available for use in entry/exit action closures.</param>
    protected abstract void Configure(StateMachine<TStateMachineState, TTrigger> machine, TState state);

    /// <summary>
    /// Creates a <see cref="StateMachine{TState,TTrigger}"/> whose current state is read from
    /// and written back to <paramref name="sagaState"/>.<see cref="IStateMachineSagaState{TStateMachineState}.CurrentState"/>.
    /// Calls <see cref="Configure"/> to apply transitions and actions before returning.
    /// </summary>
    /// <param name="sagaState">The current saga state instance.</param>
    /// <returns>A configured <see cref="StateMachine{TState,TTrigger}"/> ready to fire triggers.</returns>
    protected StateMachine<TStateMachineState, TTrigger> CreateStateMachine(TState sagaState)
    {
        var machine = new StateMachine<TStateMachineState, TTrigger>(
            () => sagaState.CurrentState,
            s => sagaState.CurrentState = s);

        Configure(machine, sagaState);

        return machine;
    }
}
