namespace InterBus.PostgreSQL.Tests;

public partial class DeadLetteringTests
{
    private sealed record DeadLetterRow(string QueueName, string Reason, string Error, DateTimeOffset DeadLetteredAt, int DeliveryCount);
}
