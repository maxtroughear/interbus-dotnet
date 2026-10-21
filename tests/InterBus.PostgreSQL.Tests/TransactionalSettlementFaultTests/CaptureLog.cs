using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace InterBus.PostgreSQL.Tests;

public partial class TransactionalSettlementFaultTests
{
    private sealed class CaptureLog : ILoggerProvider, ILogger
    {
        public ConcurrentQueue<string> Messages { get; } = new();

        public ILogger CreateLogger(string name) => this;
        public bool IsEnabled(LogLevel level) => true;
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter) => Messages.Enqueue(formatter(state, exception));
        public void Dispose()
        {
        }
    }
}
