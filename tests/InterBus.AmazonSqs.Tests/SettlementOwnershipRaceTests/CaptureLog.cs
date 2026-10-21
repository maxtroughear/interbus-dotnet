using System.Collections.Concurrent;
using InterBus.AmazonSqs.MessagePumps;
using InterBus.MessagePumps;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs.Tests;

public partial class SettlementOwnershipRaceTests
{
    private sealed class CaptureLog : ILogger<SqsPrefetchedMessage>, ILogger<DeliveryProcessor>
    {
        public ConcurrentQueue<string> Messages { get; } = new();
        public TaskCompletionSource OwnershipLost { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool IsEnabled(LogLevel level) => true;
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            Messages.Enqueue(message);
            if (message.Contains("ownership renewal failed"))
                OwnershipLost.TrySetResult();
        }
    }
}
