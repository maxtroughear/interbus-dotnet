using Microsoft.Extensions.DependencyInjection;

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
    /// <para>
    /// <strong>Prerequisites:</strong> Register
    /// <see cref="Amazon.SQS.IAmazonSQS"/> and
    /// <see cref="Amazon.SimpleNotificationService.IAmazonSimpleNotificationService"/> in
    /// the host's <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection"/>
    /// <em>before</em> calling this method. Use the AWS SDK DI extensions
    /// (<c>services.AddAWSService&lt;T&gt;()</c>) or a compatible library such as
    /// <c>LocalStack.Client.Extensions</c> for LocalStack development. This lets the
    /// application fully control client configuration, including credentials, region, and
    /// endpoint overrides.
    /// </para>
    /// <para>
    /// When <see cref="SnsSqsTransportOptions.TopologyMode"/> is set to
    /// <see cref="TopologyMode.VerifyOnly"/>, also register
    /// <see cref="Amazon.SecurityToken.IAmazonSecurityTokenService"/>. It is used to
    /// resolve the topic ARN deterministically via <c>STS GetCallerIdentity</c> without
    /// requiring <c>sns:ListTopics</c> or any write permissions.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Optional delegate to configure <see cref="SnsSqsTransportOptions"/>.
    /// </param>
    public static IInterBusBuilder AddAmazonSqsTransport(
        this IInterBusBuilder builder,
        Action<SnsSqsTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new SnsSqsTransportOptions();
        configure?.Invoke(options);

        builder.Options.SetTransportUriSchemes(SnsTransport.TopicScheme, SqsTransport.QueueScheme);
        builder.Services.AddSingleton(options);

        // Client provider holds name-sanitisation logic and caches ARNs/URLs (singleton).
        // IAmazonSQS and IAmazonSimpleNotificationService must be registered by the caller.
        builder.Services.AddSingleton<SnsSqsClientProvider>();

        // Topology setup is a singleton responsible for declaring SNS topics, SQS queues,
        // and subscriptions at startup. Separating this from the message pump keeps each
        // class focused on a single concern.
        builder.Services.AddSingleton<SnsSqsTopologySetup>();

        // SnsTransport handles snstopic:// sends (SNS fan-out publishes).
        // SqsTransport handles sqsqueue:// sends (direct SQS point-to-point).
        // Both are scoped, consistent with the RabbitMQ transport pattern.
        builder.Services.AddScoped<SnsTransport>();
        builder.Services.AddScoped<SqsTransport>();

        // Keyed registrations for transport factory scheme-based resolution.
        builder.Services.AddKeyedScoped<IInterBusTransport>(SnsTransport.TopicScheme,
            (sp, _) => sp.GetRequiredService<SnsTransport>());
        builder.Services.AddKeyedScoped<IInterBusTransport>(SqsTransport.QueueScheme,
            (sp, _) => sp.GetRequiredService<SqsTransport>());

        // Message pump runs as a background service.
        builder.Services.AddHostedService<SqsMessagePump>();

        return builder;
    }
}
