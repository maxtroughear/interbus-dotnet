using System.IO.Compression;
using InterBus.Sending;

namespace InterBus.Compression;

/// <summary>
/// Send middleware that compresses payloads using GZip.
/// </summary>
public sealed class CompressionSendMiddleware(CompressionOptions options) : ISendMiddleware
{
    public async Task InvokeAsync(
        SendContext context,
        Func<SendContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Payload.Length >= options.SizeThresholdBytes &&
            !(context.Headers.TryGetValue(InterBusHeaders.PayloadEncoding, out var existingEncoding) &&
              string.Equals(existingEncoding, "gzip", StringComparison.OrdinalIgnoreCase)))
        {
            var compressed = Compress(context.Payload, options.CompressionLevel);
            if (compressed.Length < context.Payload.Length)
            {
                context.Payload = compressed;
                context.Headers[InterBusHeaders.PayloadEncoding] = "gzip";
            }
        }

        await next(context, cancellationToken);
    }

    private static byte[] Compress(byte[] payload, CompressionLevel level)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, level, leaveOpen: true))
            gzip.Write(payload, 0, payload.Length);
        return output.ToArray();
    }
}
