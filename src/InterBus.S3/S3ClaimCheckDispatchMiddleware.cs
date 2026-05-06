using Amazon.S3;
using Amazon.S3.Model;
using InterBus.Dispatching;

namespace InterBus.S3;

/// <summary>
/// Dispatch middleware that resolves the S3 claim check for incoming messages.
/// <para>
/// When the <see cref="InterBusHeaders.S3ClaimCheckKey"/> header is present, the middleware
/// downloads the original payload from S3 using the key stored in the header and replaces
/// <see cref="DispatchContext.Payload"/> before passing control to the next step.
/// Messages without the header pass through unchanged.
/// </para>
/// </summary>
internal sealed class S3ClaimCheckDispatchMiddleware(
    IAmazonS3 s3Client,
    S3ClaimCheckOptions options)
    : IDispatchMiddleware
{
    public async Task InvokeAsync(
        DispatchContext context,
        Func<DispatchContext, CancellationToken, Task> next,
        CancellationToken cancellationToken = default)
    {
        if (context.Headers.TryGetValue(InterBusHeaders.S3ClaimCheckKey, out var key))
        {
            var response = await s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = options.BucketName,
                Key = key,
            }, cancellationToken);

            using var stream = new MemoryStream();
            await response.ResponseStream.CopyToAsync(stream, cancellationToken);
            context.Payload = stream.ToArray();
        }

        await next(context, cancellationToken);
    }
}
