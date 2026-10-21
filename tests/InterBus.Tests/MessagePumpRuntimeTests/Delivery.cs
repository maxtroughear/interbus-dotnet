using InterBus.Dispatching;
using InterBus.MessagePumps;

namespace InterBus.Tests;

public partial class MessagePumpRuntimeTests
{
    private sealed class Delivery : IDelivery
    {
        public ReadOnlyMemory<byte> Payload => ReadOnlyMemory<byte>.Empty;
        public MessageHeaders Headers { get; } = new();
        public Exception? DecodeError { get; init; }
        public string? OrderingKey { get; init; }
        public Func<Task>? Settlement { get; init; }
        public bool ThrowSettlement { get; init; }
        public bool Accepted { get; init; } = true;

        public int Settlements;
        public bool Disposed;
        public DispatchResult? Result;
        public Action? Cleanup;
        public async Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
        {
            Settlements++;
            Result = result;
            if (ThrowSettlement)
                throw new IOException("ambiguous settlement");
            if (Settlement is not null)
                await Settlement();
            return Accepted;
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            Cleanup?.Invoke();
            return ValueTask.CompletedTask;
        }
    }
}
