using InterBus.Consuming;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.AmazonSqs.Extensions;

/// <summary>
/// Extension methods for per-consumer and default Amazon SNS/SQS wire-format configuration.
/// </summary>
public static class SnsSqsConsumerBuilderExtensions
{
    extension(ConsumerBuilder builder)
    {
        /// <summary>
        /// Configures whether this consumer should receive SNS-serialised notifications on SQS
        /// (<c>RawMessageDelivery=false</c>) or raw message bodies (<c>RawMessageDelivery=true</c>).
        /// </summary>
        public ConsumerBuilder UseSnsSerialisedMessageOnSqs()
        {
            builder.Services.TryAddKeyedSingleton<ISnsSqsMessageEnvelope, SnsSerialisedMessageEnvelope>(
                builder.Descriptor.ConsumerType);

            return builder;
        }
    }
}
