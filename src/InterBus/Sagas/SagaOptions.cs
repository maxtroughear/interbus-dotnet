namespace InterBus.Sagas;

/// <summary>
/// Runtime options for a registered saga type. Registered as a singleton by
/// <see cref="IInterBusBuilder.AddSaga{TSaga,TState}"/> so that the source-generated
/// dispatch delegate can read the configured TTL without reflection.
/// </summary>
/// <typeparam name="TSaga">The saga type these options belong to.</typeparam>
public sealed class SagaOptions<TSaga> where TSaga : class
{
    /// <summary>
    /// The time-to-live applied when persisting saga state. <c>null</c> means no expiry.
    /// </summary>
    public TimeSpan? TimeToLive { get; internal set; }
}
