using Azure.Messaging.ServiceBus;

namespace InterBus.AzureServiceBus.Tests;

public partial class ManagedAdapterTests
{
    private sealed class FakeProcessor : ServiceBusProcessor
    {
        public Task Active = Task.CompletedTask;
        public bool Stopped;
        public CancellationTokenSource? SdkStop;
        public Task DeliverAsync(ProcessMessageEventArgs args) => OnProcessMessageAsync(args);
        public Task ReportAsync(ProcessErrorEventArgs args) => OnProcessErrorAsync(args);
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            SdkStop?.Cancel();
            await Active;
            Stopped = true;
        }

        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
}
