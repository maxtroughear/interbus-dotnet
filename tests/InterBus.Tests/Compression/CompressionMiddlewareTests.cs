using System.IO.Compression;
using InterBus.Compression;
using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Sending;
using Xunit;

namespace InterBus.Tests.Compression;

public sealed class CompressionMiddlewareTests
{
    [Fact]
    public async Task SendMiddleware_CompressesPayload_AndSetsHeader()
    {
        var payload = BuildRepeatingPayload();
        var context = new SendContext(
            new Uri("memoryqueue://interbus/test-queue"),
            payload,
            new MessageHeaders());
        var middleware = new CompressionSendMiddleware(new CompressionOptions { SizeThresholdBytes = 1 });

        await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask, TestContext.Current.CancellationToken);

        Assert.NotEqual(payload.Length, context.Payload.Length);
        Assert.True(context.Payload.Length < payload.Length);
        Assert.True(context.Headers.TryGetValue(InterBusHeaders.PayloadEncoding, out var encoding));
        Assert.Equal("gzip", encoding);
    }

    [Fact]
    public async Task DispatchMiddleware_Decompresses_WhenHeaderIsPresent()
    {
        var payload = BuildRepeatingPayload();
        var context = BuildDispatchContext(Compress(payload), new MessageHeaders
        {
            [InterBusHeaders.PayloadEncoding] = "gzip",
        });
        var middleware = new CompressionDispatchMiddleware();

        await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask, TestContext.Current.CancellationToken);

        Assert.Equal(payload, context.Payload);
    }

    [Fact]
    public async Task DispatchMiddleware_Decompresses_WhenPayloadIsGzipWithoutHeader()
    {
        var payload = BuildRepeatingPayload();
        var context = BuildDispatchContext(Compress(payload), new MessageHeaders());
        var middleware = new CompressionDispatchMiddleware();

        await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask, TestContext.Current.CancellationToken);

        Assert.Equal(payload, context.Payload);
    }

    [Fact]
    public async Task DispatchMiddleware_IsTolerant_WhenHeaderSaysGzipButPayloadIsPlain()
    {
        var payload = BuildRepeatingPayload();
        var context = BuildDispatchContext(payload, new MessageHeaders
        {
            [InterBusHeaders.PayloadEncoding] = "gzip",
        });
        var middleware = new CompressionDispatchMiddleware();

        await middleware.InvokeAsync(context, (_, _) => Task.CompletedTask, TestContext.Current.CancellationToken);

        Assert.Equal(payload, context.Payload);
    }

    private static DispatchContext BuildDispatchContext(byte[] payload, MessageHeaders headers)
    {
        var descriptor = new ConsumerDescriptor(typeof(object), new Uri("memoryqueue://interbus/test-consumer"));
        return new DispatchContext(descriptor, payload, headers);
    }

    private static byte[] BuildRepeatingPayload() =>
        Enumerable.Repeat("the quick brown fox jumps over the lazy dog", 500)
            .SelectMany(x => System.Text.Encoding.UTF8.GetBytes(x))
            .ToArray();

    private static byte[] Compress(byte[] payload)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
            gzip.Write(payload, 0, payload.Length);
        return output.ToArray();
    }
}
