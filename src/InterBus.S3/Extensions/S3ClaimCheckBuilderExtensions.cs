using Microsoft.Extensions.DependencyInjection;

namespace InterBus.S3.Extensions;

/// <summary>
/// Extension methods for registering the S3 claim check middleware on
/// <see cref="IInterBusBuilder"/> and <see cref="IConsumerBuilder"/>.
/// </summary>
public static class S3ClaimCheckBuilderExtensions
{
    extension(IInterBusBuilder builder)
    {
        /// <summary>
        /// Registers the S3 claim check options and send middleware at the bus level.
        /// <para>
        /// The send middleware uploads the message payload to S3 and replaces it with an empty
        /// byte array when the total estimated message size exceeds
        /// <see cref="S3ClaimCheckOptions.SizeThresholdBytes"/>. The
        /// <see cref="S3Headers.ClaimCheckKey"/> header is set to the S3 object key.
        /// </para>
        /// <para>
        /// To also receive messages that used the claim check, add dispatch middleware per
        /// consumer via <see cref="UseS3ClaimCheck"/>.
        /// </para>
        /// <para>
        /// <strong>Prerequisites:</strong> Register <see cref="Amazon.S3.IAmazonS3"/> in the
        /// host's <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection"/>
        /// <em>before</em> calling this method.
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

            // Send middleware offloads large payloads to S3 before transport delivery.
            builder.UseSendMiddleware<S3ClaimCheckSendMiddleware>();

            return builder;
        }
    }

    extension(IConsumerBuilder builder)
    {
        /// <summary>
        /// Registers the S3 claim check send and dispatch middleware for this consumer.
        /// <para>
        /// The send middleware offloads large outbound payloads to S3. The dispatch middleware
        /// transparently fetches offloaded payloads back from S3 before handing the message
        /// to the consumer handler.
        /// </para>
        /// <para>
        /// Requires <c>bus.AddS3ClaimCheck()</c> to have been called on the InterBus builder
        /// so that <see cref="S3ClaimCheckOptions"/> and <see cref="Amazon.S3.IAmazonS3"/> are
        /// registered in the DI container.
        /// </para>
        /// </summary>
        public IConsumerBuilder UseS3ClaimCheck()
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.UseDispatchMiddleware<S3ClaimCheckDispatchMiddleware>();
            builder.UseSendMiddleware<S3ClaimCheckSendMiddleware>();

            return builder;
        }
    }
}
