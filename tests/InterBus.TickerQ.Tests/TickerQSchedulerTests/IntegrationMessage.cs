namespace InterBus.TickerQ.Tests;

public sealed partial class TickerQSchedulerTests
{
    internal record IntegrationMessage(Guid Id, string Payload);
}
