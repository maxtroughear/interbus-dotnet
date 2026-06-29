using System.IO.Compression;
using InterBus.Dispatching;

namespace InterBus.Compression;

/// <summary>
/// Dispatch middleware that decompresses incoming GZip payloads.
/// </summary>
public sealed class CompressionDispatchMiddleware : IDispatchMiddleware
{
    public async Task InvokeAsync(
        DispatchContext context,
        Func<DispatchContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        var hasGzipHeader = context.Headers.TryGetValue(InterBusHeaders.PayloadEncoding, out var encoding) &&
                            string.Equals(encoding, "gzip", StringComparison.OrdinalIgnoreCase);
        if (hasGzipHeader || IsGZipPayload(context.Payload))
        {
            try
            {
                context.Payload = Decompress(context.Payload);
            }
            catch (InvalidDataException)
            {
                // Keep payload unchanged so mixed compressed/non-compressed publishers can coexist safely.
            }
        }

        await next(context, cancellationToken);
    }

    private static bool IsGZipPayload(byte[] payload) =>
        payload.Length >= 2 &&
        payload[0] == 0x1F &&
        payload[1] == 0x8B;

    private static byte[] Decompress(byte[] payload)
    {
        using var input = new MemoryStream(payload);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        return output.ToArray();
    }
}
