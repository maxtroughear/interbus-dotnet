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
    /// <inheritdoc />
    public Task<string> ScheduleAsync(Uri destination, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var request = new ScheduledDeliveryRequest(destination, payload, headers);
        var jobId = backgroundJobClient.Create<HangfireDeliverFunction>(
            function => function.InvokeAsync(request, JobCancellationToken.Null),
            new ScheduledState(scheduledAt.UtcDateTime));

        return Task.FromResult(jobId);
    }

    /// <inheritdoc />
    public Task CancelAsync(string scheduleId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        backgroundJobClient.Delete(scheduleId);
        return Task.CompletedTask;
    }
}
