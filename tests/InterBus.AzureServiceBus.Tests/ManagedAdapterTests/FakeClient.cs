using Azure.Messaging.ServiceBus;

namespace InterBus.AzureServiceBus.Tests;

public partial class ManagedAdapterTests
{
    private sealed class FakeClient(FakeProcessor processor) : ServiceBusClient
    {
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public override ServiceBusProcessor CreateProcessor(string queueName, ServiceBusProcessorOptions options)
        {
            Assert.That(options.AutoCompleteMessages, Is.False);
            return processor;
        }
    }
}
