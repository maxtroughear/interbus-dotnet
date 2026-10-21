using InterBus.Dispatching;
using InterBus.MessagePumps;

namespace InterBus.InMemory;

internal sealed partial class InMemoryMessagePump
{
    private sealed class MemoryDelivery(InMemoryEnvelope envelope) : IDelivery
    {
        public ReadOnlyMemory<byte> Payload => envelope.Payload;
        public MessageHeaders Headers => envelope.Headers;
        public bool Released { get; private set; }

        public Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
        {
            Released = result.Kind == DispatchResultKind.Release;
            return Task.FromResult(true);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
