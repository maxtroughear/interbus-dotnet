using InterBus.AmazonSqs.MessagePumps;
using InterBus.MessagePumps;
using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.AmazonSqs.Extensions;

/// <summary>
/// Extension methods for configuring the Amazon SNS/SQS transport on an
/// <see cref="InterBusBuilder"/>.
/// </summary>
public static class SnsSqsTransportBuilderExtensions
{
    /// <param name="builder">The InterBus builder.</param>
    extension(InterBusBuilder builder)
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
        /// <see cref="TopologyMode.ResolveOnly"/>, also register
        /// <see cref="Amazon.SecurityToken.IAmazonSecurityTokenService"/>. It is used to
        /// resolve the topic ARN deterministically via <c>STS GetCallerIdentity</c> without
        /// requiring <c>sns:ListTopics</c> or any write permissions.
        /// </para>
        /// </summary>
        /// <param name="configure">
        /// Optional delegate to configure <see cref="SnsSqsTransportOptions"/>.
        /// </param>
        public InterBusBuilder AddAmazonSqsTransport(Action<SnsSqsTransportOptions>? configure = null)
        {
            var options = new SnsSqsTransportOptions();
            configure?.Invoke(options);
            options.Validate();

            builder.Options.SetTransportUriSchemes(SnsTransport.TopicScheme, SqsTransport.QueueScheme);
            builder.Services.AddSingleton(options);
            builder.Services.AddSingleton<SnsSqsNaming>();
            builder.Services.AddSingleton<SnsSqsTopologyBuilder>();
            builder.Services.AddSingleton<ITopologyBuilder>(sp => sp.GetRequiredService<SnsSqsTopologyBuilder>());
            builder.Services.AddSingleton<SnsSqsMessageEnvelope>();
            builder.Services.AddSingleton<SnsSerialisedMessageEnvelope>(); // TODO: Remove, doesn't seem used directly
            builder.Services.AddSingleton<ISnsSqsMessageEnvelope, SnsSqsMessageEnvelope>();
            builder.Services.AddKeyedScoped<ITransport, SnsTransport>(SnsTransport.TopicScheme);
            builder.Services.AddKeyedScoped<ITransport, SqsTransport>(SqsTransport.QueueScheme);
            builder.Services.AddSingleton<IVirtualQueueHost, SqsVirtualQueueHost>();
            builder.Services.AddSingleton<IReplyEndpointFactory, VirtualQueueReplyEndpointFactory>();
            builder.Services.AddSingleton<IConsumerSourceFactory, SqsQueueMessagePumpFactory>();


            return builder;
        }
    }
}
