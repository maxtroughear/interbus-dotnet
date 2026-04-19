using InterBus.Publisher;
using InterBus.Send;

namespace InterBus.Scheduler;

/// <summary>
/// Carries the state of a single scheduled-message delivery through the
/// <see cref="IScheduleMiddleware"/> pipeline, executed by
/// <see cref="SchedulePipelineExecutor"/> when the scheduled job fires.
/// Unlike <see cref="PublishContext{T}"/>, this context operates on an already-serialized
/// payload, allowing middleware to inspect or mutate the raw bytes and headers
/// (e.g. to implement the Claim Check pattern for large scheduled payloads).
/// </summary>
public sealed class ScheduleContext(Uri destination, byte[] payload, MessageHeaders headers, TimeSpan? delay)
    : SendContext(destination, payload, headers, delay);
