using Azure.Messaging.ServiceBus;
using InterBus.Dispatching;

namespace InterBus.AzureServiceBus.Tests;

public partial class SessionAdapterTests
{
    private sealed class Receiver : ServiceBusSessionReceiver
    {
        public bool LostLock;
        public int Calls;
        public DispatchResultKind? Outcome;
        public override string SessionId => "session";

        private Task Settle(DispatchResultKind kind)
        {
            Calls++;
            if (LostLock)
                throw new ServiceBusException("session lost", ServiceBusFailureReason.SessionLockLost);
            Outcome = kind;
            return Task.CompletedTask;
        }

        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default) => Settle(DispatchResultKind.Success);
        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default) => Settle(DispatchResultKind.Release);
        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, string? deadLetterReason = null, string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default) => Settle(DispatchResultKind.DeadLetter);
    }
}
