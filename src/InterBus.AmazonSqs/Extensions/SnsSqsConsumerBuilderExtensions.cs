using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.AmazonSqs.Extensions;

/// <summary>
/// Extension methods for per-consumer and default Amazon SNS/SQS wire-format configuration.
/// </summary>
public static class SnsSqsConsumerBuilderExtensions
{
    extension(IConsumerBuilder builder)
    {
        /// <summary>
        /// Configures whether this consumer should receive SNS-serialised notifications on SQS
        /// (<c>RawMessageDelivery=false</c>) or raw message bodies (<c>RawMessageDelivery=true</c>).
        /// </summary>
        public IConsumerBuilder UseSnsSerialisedMessageOnSqs(bool enabled = true)
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.Services.Add(ServiceDescriptor.KeyedSingleton(
                typeof(ISnsSqsMessageEnvelope),
                builder.ConsumerType,
                enabled
                    ? typeof(SnsSerialisedMessageEnvelope)
                    : typeof(SnsSqsMessageEnvelope)));

            return builder;
        }
    }

    extension(IInterBusBuilder builder)
    {
        /// <summary>
        /// Sets SNS-serialised notifications (<c>RawMessageDelivery=false</c>) as the default
        /// SNS/SQS wire format for all consumers that do not override it.
        /// </summary>
        public IInterBusBuilder UseSnsSerialisedMessageOnSqsByDefault()
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.Services.Replace(ServiceDescriptor.Singleton<ISnsSqsMessageEnvelope, SnsSerialisedMessageEnvelope>());
            return builder;
        }
    }
}
