using TickerQ.Utilities;
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
        var scheduleId = Guid.CreateVersion7();
        await manager.AddAsync(new TimeTickerEntity
        {
            Id = scheduleId,
            Function = TickerQDeliverFunction.FunctionName,
            ExecutionTime = scheduledAt.UtcDateTime,
            Request = TickerHelper.CreateTickerRequest(new ScheduledDeliveryRequest(destination, payload, headers))
        }, cancellationToken);
        return scheduleId.ToString();
    }

    /// <inheritdoc/>
    public Task CancelAsync(string scheduleId, CancellationToken cancellationToken) =>
        manager.DeleteAsync(Guid.Parse(scheduleId), cancellationToken);
}
