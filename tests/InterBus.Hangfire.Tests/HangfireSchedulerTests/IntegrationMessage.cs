namespace InterBus.Hangfire.Tests;

public sealed partial class HangfireSchedulerTests
{
    internal record IntegrationMessage(Guid Id, string Payload);
}
