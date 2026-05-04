namespace InterBus.Scheduling;

/// <summary>
/// Consumer that processes <see cref="CancelScheduleCommand"/> messages published by
/// <see cref="PublishMessageScheduler"/>. Enlists the registered
/// <see cref="IMessageSchedulingProvider"/> to cancel the scheduled delivery.
/// <para>
/// Register via <c>bus.AddSchedulingConsumer()</c> alongside an
/// <see cref="IMessageSchedulingProvider"/> (e.g. via <c>AddTickerQScheduler()</c>).
/// </para>
/// </summary>
internal sealed class CancelSchedulingConsumer(IMessageSchedulingProvider provider) :
    IConsumer<CancelScheduleCommand>
{
    public Task Consume(CancelScheduleCommand message, IConsumeContext context,
        CancellationToken cancellationToken = default) =>
        provider.CancelAsync(message.ScheduleId, cancellationToken);
}
