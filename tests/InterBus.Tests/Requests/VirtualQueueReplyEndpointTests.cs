using System.Threading.Channels;
using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests.Requests;

public class VirtualQueueReplyEndpointTests
{
    [Fact]
    public async Task CreateAsync_UsesConfiguredQueueSchemeAndUnregistersVirtualQueue()
    {
        var host = new FakeVirtualQueueHost("reply-queue", "vq-123");
        var options = new InterBusOptions();
        options.SetTransportUriSchemes("topic-scheme", "queue-scheme");

        await using var provider = new ServiceCollection().BuildServiceProvider();
        var factory = new VirtualQueueReplyEndpointFactory<FakeVirtualQueueHost>(
            host,
            options,
            (IServiceScopeFactory)provider);

        var endpoint = await factory.CreateAsync();

        Assert.Equal("queue-scheme", endpoint.Address.Scheme);
        Assert.Equal("reply-queue", RoutingConventions.GetDestinationName(endpoint.Address));
        Assert.Equal("vq=vq-123", endpoint.Address.Query.TrimStart('?'));

        await endpoint.DisposeAsync();

        Assert.Equal("vq-123", host.UnregisteredVirtualQueueId);
    }

    [Fact]
    public async Task ReceiveAsync_WhenChannelCompletesWithoutReply_ThrowsSharedError()
    {
        var channel = Channel.CreateUnbounded<(byte[] Payload, MessageHeaders Headers)>();
        channel.Writer.TryComplete();

        await using var provider = new ServiceCollection().BuildServiceProvider();
        var endpoint = new VirtualQueueReplyEndpoint(
            new FakeVirtualQueueHost("reply-queue", "vq-123", channel),
            (IServiceScopeFactory)provider,
            new Uri("queue://reply-queue?vq=vq-123"),
            "vq-123",
            channel.Reader);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => endpoint.ReceiveAsync<object>());

        Assert.Equal("InterBus: reply channel closed without receiving a Object", ex.Message);
    }

    private sealed class FakeVirtualQueueHost(
        string queueName,
        string virtualQueueId,
        Channel<(byte[] Payload, MessageHeaders Headers)>? channel = null)
        : IVirtualQueueHost
    {
        private readonly Channel<(byte[] Payload, MessageHeaders Headers)> _channel =
            channel ?? Channel.CreateUnbounded<(byte[] Payload, MessageHeaders Headers)>();

        public string? UnregisteredVirtualQueueId { get; private set; }

        public Task<VirtualQueueRegistration> RegisterAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new VirtualQueueRegistration(queueName, virtualQueueId, _channel.Reader));

        public void Unregister(string virtualQueueId)
            => UnregisteredVirtualQueueId = virtualQueueId;
    }
}
