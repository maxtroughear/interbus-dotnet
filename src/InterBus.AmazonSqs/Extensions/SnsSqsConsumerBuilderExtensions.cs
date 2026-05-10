using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Extensions;

/// <summary>
/// Extension methods for per-consumer Amazon SNS/SQS configuration.
/// </summary>
public static class SnsSqsConsumerBuilderExtensions
{
    extension(IConsumerBuilder builder)
    {
        /// <summary>
        /// Configures whether this consumer should receive SNS-serialised messages on SQS
        /// (<c>RawMessageDelivery=false</c>) or raw message bodies (<c>RawMessageDelivery=true</c>).
        /// </summary>
        public IConsumerBuilder UseSnsSerialisedMessageOnSqs(bool enabled = true)
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.Descriptor.UseSnsSerialisedMessageOnSqs = enabled;
            return builder;
        }

        /// <summary>
        /// Registers a consumer-specific message serializer resolved by keyed DI using this
        /// consumer type as the key.
        /// </summary>
        public IConsumerBuilder UseMessageSerializer<TSerializer>()
            where TSerializer : class, IMessageSerializer
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.Services.Add(ServiceDescriptor.KeyedSingleton(
                typeof(IMessageSerializer),
                builder.Descriptor.ConsumerType,
                typeof(TSerializer)));

            return builder;
        }

        /// <summary>
        /// Registers a consumer-specific serializer instance resolved by keyed DI using this
        /// consumer type as the key.
        /// </summary>
        public IConsumerBuilder UseMessageSerializer(IMessageSerializer serializer)
        {
            ArgumentNullException.ThrowIfNull(builder);
            ArgumentNullException.ThrowIfNull(serializer);

            builder.Services.Add(ServiceDescriptor.KeyedSingleton(
                typeof(IMessageSerializer),
                builder.Descriptor.ConsumerType,
                serializer));

            return builder;
        }
    }
}
