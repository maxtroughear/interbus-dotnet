using InterBus.Consuming;

namespace InterBus;

/// <summary>
/// Provides access to the <see cref="IConsumeContext"/> for the message currently being
/// processed within this DI scope. The value is set by the InterBus infrastructure before
/// the consumer's <c>Consume</c> method is invoked, and is <c>null</c> when no consume
/// operation is in progress.
/// <para>
/// Inject <see cref="IConsumeContextAccessorOld"/> in publish, send, or schedule middleware
/// to inspect or act on the triggering consume context — for example, to propagate
/// custom headers from the consumed message to any outgoing messages.
/// </para>
/// </summary>
public interface IConsumeContextAccessorOld
{
    /// <summary>
    /// The <see cref="IConsumeContext"/> for the message currently being processed, or
    /// <c>null</c> if no consume operation is active in this scope.
    /// </summary>
    /// <remarks>
    /// The setter is used exclusively by the InterBus infrastructure
    /// (<see cref="ConsumePipelineExecutor"/>) to initialise the value before
    /// each consumer dispatch. Application code should treat this property as read-only.
    /// </remarks>
    IConsumeContext? ConsumeContext { get; set; }
}
