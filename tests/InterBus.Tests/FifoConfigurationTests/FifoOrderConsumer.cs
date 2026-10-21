namespace InterBus.Tests;

public sealed partial class FifoConfigurationTests
{
    // ── Helpers ─────────────────────────────────────────────────────────────
    internal sealed class FifoOrderConsumer : IConsumer<FifoTestOrderPlaced>
    {
        public Task Consume(FifoTestOrderPlaced message, IConsumeContext context, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
