namespace InterBus.Sagas;

/// <summary>
/// Runtime options for a registered saga type
/// </summary>
/// <typeparam name="TSaga">The saga type these options belong to.</typeparam>
public sealed class SagaOptions<TSaga>
{
    /// <summary>
    /// The time-to-live applied when persisting saga state. <c>null</c> means no expiry.
    /// </summary>
    public TimeSpan? TimeToLive { get; internal set; }
}
