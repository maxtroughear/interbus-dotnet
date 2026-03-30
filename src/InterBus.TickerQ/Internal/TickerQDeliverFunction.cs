using Microsoft.Extensions.DependencyInjection;
using TickerQ.Utilities;
using TickerQ.Utilities.Base;
using TickerQ.Utilities.Entities;
using TickerQ.Utilities.Interfaces;

namespace InterBus.TickerQ.Internal;

/// <summary>
/// Hosts the TickerQ function delegate that delivers a scheduled InterBus message to the transport.
/// The function is registered programmatically with <see cref="TickerFunctionProvider"/> during
/// DI setup (before the host starts) so TickerQ can find it by name.
/// </summary>
internal static class TickerQDeliverFunction
{
    /// <summary>
    /// The TickerQ function name used as the <c>Function</c> property on every scheduled
    /// <c>TimeTickerEntity</c> that InterBus creates.
    /// </summary>
    public const string FunctionName = "interbus-deliver";

    /// <summary>
    /// TickerQ function delegate. Reads the <see cref="ScheduledMessageRequest"/> from the
    /// persistence store, then dispatches to <see cref="IInterBusTransport"/>.
    /// </summary>
    public static async Task InvokeAsync(
        CancellationToken cancellationToken,
        IServiceProvider serviceProvider,
        TickerFunctionContext context)
    {
        var persistenceProvider =
            serviceProvider.GetRequiredService<ITickerPersistenceProvider<TimeTickerEntity, CronTickerEntity>>();

        var requestBytes = await persistenceProvider.GetTimeTickerRequest(context.Id, cancellationToken);
        if (requestBytes is null)
            return;

        var request = TickerHelper.ReadTickerRequest<ScheduledMessageRequest>(requestBytes);
        var transport = serviceProvider.GetRequiredService<IInterBusTransport>();
        var headers = new MessageHeaders(request.Headers);

        if (request.IsTopic)
            await transport.PublishToTopicAsync(request.Destination, request.Payload, headers, cancellationToken);
        else
            await transport.SendToQueueAsync(request.Destination, request.Payload, headers, cancellationToken);
    }
}
