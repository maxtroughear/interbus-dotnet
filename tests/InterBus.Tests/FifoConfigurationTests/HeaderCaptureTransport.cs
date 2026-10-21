using InterBus.Transporting;

namespace InterBus.Tests;

public sealed partial class FifoConfigurationTests
{
    private sealed class HeaderCaptureTransport : ITransport
    {
        public MessageHeaders? LastHeaders { get; private set; }

        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
        {
            LastHeaders = context.Headers;
            return Task.CompletedTask;
        }
    }
}
