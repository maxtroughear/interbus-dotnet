using InterBus.Sending;

namespace InterBus.Tests.InMemory;

public partial class InMemoryOutboxTests
{
    internal class SimulateFailure : ISendMiddleware
    {
        private bool _shouldFail = true;
        public Task InvokeAsync(SendContext context, Func<SendContext, CancellationToken, Task> next, CancellationToken cancellationToken = default)
        {
            if (!_shouldFail)
                return next(context, cancellationToken);
            _shouldFail = false;
            throw new Exception("Simulated failure");
        }
    }
}
