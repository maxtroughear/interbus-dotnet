using System.Collections.Concurrent;
using Hangfire;
using Hangfire.States;

namespace InterBus.Hangfire;

/// <summary>
/// <see cref="IMessageSchedulingProvider"/> implementation that uses Hangfire for deferred
/// delivery of InterBus messages at their specified schedule time.
/// <para>
/// Register via <c>builder.AddHangfireScheduler()</c> on the <see cref="InterBusBuilder"/>.
/// </para>
/// </summary>
public sealed class HangfireMessageScheduler(IBackgroundJobClient backgroundJobClient) : IMessageSchedulingProvider
{
    private readonly ConcurrentDictionary<Guid, string> _scheduleToJob = new();

    /// <inheritdoc />
    public Task ScheduleAsync(Guid scheduleId, Uri destination, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken)
    {
        var request = new ScheduledDeliveryRequest(destination, payload, headers);
        var jobId = backgroundJobClient.Create<HangfireDeliverFunction>(
            function => function.InvokeAsync(request),
            new ScheduledState(scheduledAt.UtcDateTime));

        _scheduleToJob[scheduleId] = jobId;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task CancelAsync(Guid scheduleId, CancellationToken cancellationToken)
    {
        if (_scheduleToJob.TryRemove(scheduleId, out var jobId))
            backgroundJobClient.Delete(jobId);

        return Task.CompletedTask;
    }
}
