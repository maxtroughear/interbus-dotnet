using Azure.Messaging.ServiceBus;

namespace InterBus.AzureServiceBus.Tests;

public partial class SessionAdapterTests
{
    private sealed class Client(SessionProcessor processor) : ServiceBusClient
    {
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public override ServiceBusSessionProcessor CreateSessionProcessor(string queueName, ServiceBusSessionProcessorOptions options)
        {
            Assert.That(options.MaxConcurrentSessions, Is.EqualTo(2));
            Assert.That(options.MaxConcurrentCallsPerSession, Is.EqualTo(1));
            Assert.That(options.AutoCompleteMessages, Is.False);
            Assert.That(options.MaxAutoLockRenewalDuration, Is.EqualTo(TimeSpan.FromHours(12)));
            return processor;
        }
    }
}
