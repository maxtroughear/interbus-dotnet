namespace InterBus.MessagePumps;

internal static class SourceHealth
{
    internal static readonly Task<Exception> Never = new TaskCompletionSource<Exception>().Task;
}
