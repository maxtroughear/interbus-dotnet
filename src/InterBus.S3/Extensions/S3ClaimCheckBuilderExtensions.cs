using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.S3.Extensions;

/// <summary>
/// Extension methods for registering the S3 claim check middleware on an
/// <see cref="IInterBusBuilder"/>.
/// </summary>
public static class S3ClaimCheckBuilderExtensions
{
    extension(IInterBusBuilder builder)
    {
        /// <summary>
        /// Registers the S3 claim check send and dispatch middleware.
        /// <para>
        /// The send middleware uploads the message payload to S3 and replaces it with an empty
        /// byte array when the total estimated message size exceeds
        /// <see cref="S3ClaimCheckOptions.SizeThresholdBytes"/>. The
        /// <see cref="InterBusHeaders.S3ClaimCheckKey"/> header is set to the S3 object key.
        /// </para>
        /// <para>
        /// The dispatch middleware checks for the <see cref="InterBusHeaders.S3ClaimCheckKey"/>
        /// header on every incoming message. When present, it downloads the original payload from
        /// S3 and replaces <see cref="InterBus.Dispatching.DispatchContext.Payload"/> before
        /// passing control to the next middleware step.
        /// </para>
        /// <para>
        /// <strong>Prerequisites:</strong> Register <see cref="Amazon.S3.IAmazonS3"/> in the
        /// host's <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection"/>
        /// <em>before</em> calling this method. Use the AWS SDK DI extensions
        /// (<c>services.AddAWSService&lt;IAmazonS3&gt;()</c>) or a compatible library such as
        /// <c>LocalStack.Client.Extensions</c> for LocalStack development.
        /// </para>
        /// </summary>
        /// <param name="configure">
        /// Optional delegate to configure <see cref="S3ClaimCheckOptions"/>.
        /// At minimum <see cref="S3ClaimCheckOptions.BucketName"/> must be set.
        /// </param>
        public IInterBusBuilder AddS3ClaimCheck(Action<S3ClaimCheckOptions>? configure = null)
        {
            ArgumentNullException.ThrowIfNull(builder);

            var options = new S3ClaimCheckOptions();
            configure?.Invoke(options);

            builder.Services.AddSingleton(options);

            // Dispatch middleware resolves the claim check on every incoming message.
            builder.Services.AddScoped<IDispatchMiddleware, S3ClaimCheckDispatchMiddleware>();

            // Send middleware offloads large payloads to S3 before transport delivery.
            builder.UseSendMiddleware<S3ClaimCheckSendMiddleware>();

            return builder;
        }
    }
}
