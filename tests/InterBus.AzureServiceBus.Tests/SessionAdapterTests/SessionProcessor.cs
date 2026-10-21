using Azure.Messaging.ServiceBus;

namespace InterBus.AzureServiceBus.Tests;

public partial class SessionAdapterTests
{
    private sealed class SessionProcessor : ServiceBusSessionProcessor
    {
        public Task Active = Task.CompletedTask;
        public CancellationTokenSource? SdkStop;
        public Exception? StartFailure;
        public bool Closed;
        public TaskCompletionSource Stopping { get; } = Signal();

        private readonly ServiceBusClient _mockClient;
        public SessionProcessor() : this(new ServiceBusClient("Endpoint=sb://unit-test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA=="))
        {
        }

        private SessionProcessor(ServiceBusClient client) : base(client, "sessions", new ServiceBusSessionProcessorOptions())
        {
            _mockClient = client;
        }

        public Task DeliverAsync(ProcessSessionMessageEventArgs args) => OnProcessSessionMessageAsync(args);
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) => StartFailure is null ? Task.CompletedTask : Task.FromException(StartFailure);
        public override async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            SdkStop?.Cancel();
            Stopping.TrySetResult();
            await Active;
        }

        public override Task CloseAsync(CancellationToken cancellationToken = default)
        {
            Assert.That(Active.IsCompleted, Is.True);
            Closed = true;
            return _mockClient.DisposeAsync().AsTask();
        }
    }
}
