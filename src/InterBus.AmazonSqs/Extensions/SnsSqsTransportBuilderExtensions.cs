using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs.Extensions;

/// <summary>
/// Extension methods for configuring the Amazon SNS/SQS transport on an
/// <see cref="IInterBusBuilder"/>.
/// </summary>
public static class SnsSqsTransportBuilderExtensions
{
    /// <summary>
    /// Use Amazon SNS/SQS as the InterBus message transport.
    /// <para>
    /// Topic (fan-out) messages are published to an SNS topic named
    /// <c>{TopicPrefix}{topicName}</c>. Every SQS queue subscribed to the topic via the
    /// message pump receives a copy.
    /// </para>
    /// <para>
    /// Queue (point-to-point) messages are sent directly to an SQS queue named
    /// <c>{QueuePrefix}{queueName}</c>, delivering the message to exactly one consumer.
    /// </para>
    /// <para>
    /// The <see cref="SqsMessagePump"/> hosted service declares all required SNS topics,
    /// SQS queues, and subscriptions at startup, then long-polls each consumer's SQS queue
    /// for incoming messages.
    /// </para>
    /// <para>
    /// Delayed delivery is supported for direct SQS sends (up to 900 seconds). SNS topic
    /// publishes do not support delay; the delay value is ignored for topic sends.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Optional delegate to configure <see cref="SnsSqsTransportOptions"/>. At minimum set
    /// <see cref="SnsSqsTransportOptions.Region"/> and optionally
    /// <see cref="SnsSqsTransportOptions.ServiceUrl"/> for local development with LocalStack.
    /// </param>
    public static IInterBusBuilder AddAmazonSqsTransport(
        this IInterBusBuilder builder,
        Action<SnsSqsTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new SnsSqsTransportOptions();
        configure?.Invoke(options);

        builder.Options.SetTransportUriSchemes(SnsTransport.TopicScheme, SnsTransport.QueueScheme);
        builder.Services.AddSingleton(options);

        // Client provider manages SNS/SQS clients and caches ARNs/URLs (singleton).
        builder.Services.AddSingleton<SnsSqsClientProvider>();

        // SnsTransport is scoped (consistent with RabbitMQ transport pattern).
        builder.Services.AddScoped<SnsTransport>(sp => new SnsTransport(
            sp.GetRequiredService<SnsSqsClientProvider>()));

        // Keyed registrations for transport factory scheme-based resolution.
        builder.Services.AddKeyedScoped<IInterBusTransport>(SnsTransport.TopicScheme,
            (sp, _) => sp.GetRequiredService<SnsTransport>());
        builder.Services.AddKeyedScoped<IInterBusTransport>(SnsTransport.QueueScheme,
            (sp, _) => sp.GetRequiredService<SnsTransport>());

        // Message pump runs as a background service.
        builder.Services.AddHostedService<SqsMessagePump>();

        return builder;
    }
}
