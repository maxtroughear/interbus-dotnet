using TickerQ.Utilities.Entities;
using TickerQ.Utilities.Interfaces.Managers;

namespace InterBus.TickerQ;

/// <summary>
/// <see cref="IMessageSchedulingProvider"/> implementation that uses TickerQ to persist and fire
/// scheduled InterBus messages at their specified delivery time.
/// <para>
/// Register via <c>builder.AddTickerQScheduler()</c> on the <see cref="InterBusBuilder"/>.
/// </para>
/// </summary>
public sealed class TickerQMessageScheduler(ITimeTickerManager<TimeTickerEntity> manager) : IMessageSchedulingProvider
{
    /// <inheritdoc/>
    public async Task<string> ScheduleAsync(Uri destination, byte[] payload, MessageHeaders headers,
        DateTimeOffset scheduledAt, CancellationToken cancellationToken)
    {
        var result = await manager.AddAsync<TickerQDeliverFunction, ScheduledDeliveryRequest>(
            scheduledAt.UtcDateTime,
            new ScheduledDeliveryRequest(destination, payload, headers),
            cancellationToken);

        if (result is null)
            throw new InvalidOperationException("TickerQ returned a null result");

        if (result.Exception is not null)
            throw result.Exception;

        if (!result.IsSucceeded)
            throw new InvalidOperationException("Failed to schedule message delivery");

        if (result.Result is null)
            throw new InvalidOperationException("TickerQ returned a successful result but the result is null");

        return result.Result.Id.ToString();
    }

    /// <inheritdoc/>
    public Task CancelAsync(string scheduleId, CancellationToken cancellationToken) =>
        manager.DeleteAsync(Guid.Parse(scheduleId), cancellationToken);
}
