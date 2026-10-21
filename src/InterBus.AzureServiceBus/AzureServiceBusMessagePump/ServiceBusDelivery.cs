using Azure.Messaging.ServiceBus;
using InterBus.Dispatching;
using InterBus.MessagePumps;

namespace InterBus.AzureServiceBus;

internal sealed partial class AzureServiceBusMessagePump
{
    private sealed class ServiceBusDelivery : IDelivery
    {
        private readonly Func<DispatchResult, CancellationToken, Task> _settle;
        public ReadOnlyMemory<byte> Payload { get; }
        public MessageHeaders Headers { get; } = new();

        public ServiceBusDelivery(ServiceBusReceivedMessage message, Func<DispatchResult, CancellationToken, Task> settle)
        {
            Payload = message.Body.ToArray();
            _settle = settle;
            foreach (var (key, value) in message.ApplicationProperties)
                if (value is not null)
                    Headers[key] = value.ToString()!;
        }

        public async Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
        {
            try
            {
                await _settle(result, cancellationToken);
                return true;
            }
            catch (ServiceBusException error) when (error.Reason is ServiceBusFailureReason.MessageLockLost or ServiceBusFailureReason.SessionLockLost)
            {
                return false;
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
