using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.Extensions;

/// <summary>
/// Generic extension methods for configuring individual consumers.
/// </summary>
public static class ConsumerBuilderExtensions
{
    extension(IConsumerBuilder builder)
    {
        /// <summary>
        /// Registers a consumer-specific serializer resolved by keyed DI using this consumer
        /// type as the key.
        /// </summary>
        public IConsumerBuilder UseMessageSerializer<
            [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
            TSerializer>()
            where TSerializer : class, IMessageSerializer
        {
            ArgumentNullException.ThrowIfNull(builder);

            builder.Services.Add(ServiceDescriptor.KeyedSingleton(
                typeof(IMessageSerializer),
                builder.ConsumerType,
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
                builder.ConsumerType,
                serializer));

            return builder;
        }
    }
}
