namespace InterBus.Tests;

public partial class MessagePumpRuntimeTests
{
    private sealed class ScopeMarker : IDisposable
    {
        public bool Disposed;
        public void Dispose() => Disposed = true;
    }
}
