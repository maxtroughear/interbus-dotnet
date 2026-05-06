using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using InterBus.Sending;

namespace InterBus.S3;

/// <summary>
/// Send middleware that applies the S3 claim check pattern to large messages.
/// <para>
/// When the estimated total message size (payload + headers) exceeds
/// <see cref="S3ClaimCheckOptions.SizeThresholdBytes"/>, the payload is uploaded to S3
/// and replaced with an empty byte array. The <see cref="S3Headers.ClaimCheckKey"/>
/// header is set to the S3 object key so the receiving end can retrieve the original payload.
/// </para>
/// </summary>
internal sealed class S3ClaimCheckSendMiddleware(
    IAmazonS3 s3Client,
    S3ClaimCheckOptions options)
    : ISendMiddleware
{
    public async Task InvokeAsync(
        SendContext context,
        Func<SendContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        var totalSize = CalculateSize(context);

        if (totalSize > options.SizeThresholdBytes)
        {
            var key = $"{options.KeyPrefix}{Guid.CreateVersion7()}";

            using var stream = new MemoryStream(context.Payload);
            await s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = options.BucketName,
                Key = key,
                InputStream = stream,
                ContentType = "application/octet-stream",
            }, cancellationToken);

            context.Headers[S3Headers.ClaimCheckKey] = key;
            context.Payload = [];
        }

        await next(context, cancellationToken);
    }

    private static int CalculateSize(SendContext context)
    {
        var headerSize = 0;
        foreach (var (key, value) in context.Headers)
            headerSize += Encoding.UTF8.GetByteCount(key) + Encoding.UTF8.GetByteCount(value);

        return context.Payload.Length + headerSize;
    }
}
