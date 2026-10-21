using Azure.Messaging.ServiceBus;
using InterBus.Dispatching;

namespace InterBus.AzureServiceBus.Tests;

public partial class ManagedAdapterTests
{
    private sealed class FakeReceiver : ServiceBusReceiver
    {
        public bool LoseLock;
        public DispatchResultKind? Outcome;
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            if (LoseLock)
                throw new ServiceBusException("lost", ServiceBusFailureReason.MessageLockLost);
            Outcome = DispatchResultKind.Success;
            return Task.CompletedTask;
        }

        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default)
        {
            Outcome = DispatchResultKind.Release;
            return Task.CompletedTask;
        }

        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, string? deadLetterReason = null, string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default)
        {
            Outcome = DispatchResultKind.DeadLetter;
            return Task.CompletedTask;
        }
    }
}
