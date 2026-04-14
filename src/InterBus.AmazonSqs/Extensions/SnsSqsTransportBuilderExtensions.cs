using Amazon.Extensions.NETCore.Setup;
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
    /// AWS credentials and region are resolved by the AWS SDK using the standard provider
    /// chain (environment variables, shared credentials file, IAM instance profile, etc.).
    /// To supply explicit credentials or a specific region, call
    /// <see cref="Microsoft.Extensions.DependencyInjection.ServiceCollectionExtensions.AddDefaultAWSOptions"/>
    /// on the host's <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection"/>
    /// <em>before</em> calling this method.
    /// </para>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="configure">
    /// Optional delegate to configure <see cref="SnsSqsTransportOptions"/>. Set
    /// <see cref="SnsSqsTransportOptions.ServiceUrl"/> when targeting a local emulator
    /// such as LocalStack.
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

        // Register SNS and SQS clients via the AWS SDK DI extensions so that credentials and
        // region are resolved automatically through the standard provider chain (environment
        // variables, shared credentials file, IAM instance profile, etc.).
        //
        // When a ServiceUrl override is configured (e.g. LocalStack), a factory is used that
        // inherits credentials and region from the DI-registered AWSOptions (if any) while
        // applying the endpoint override. In the common case (no ServiceUrl) the simpler
        // AddAWSService<T>() is used which reads AWSOptions from DI directly.
        if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
        {
            var serviceUrl = options.ServiceUrl;

            builder.Services.AddSingleton<Amazon.SimpleNotificationService.IAmazonSimpleNotificationService>(
                sp => CreateServiceClientWithServiceUrl<Amazon.SimpleNotificationService.IAmazonSimpleNotificationService>(sp, serviceUrl));

            builder.Services.AddSingleton<Amazon.SQS.IAmazonSQS>(
                sp => CreateServiceClientWithServiceUrl<Amazon.SQS.IAmazonSQS>(sp, serviceUrl));
        }
        else
        {
            builder.Services.AddAWSService<Amazon.SimpleNotificationService.IAmazonSimpleNotificationService>();
            builder.Services.AddAWSService<Amazon.SQS.IAmazonSQS>();
        }

        // Client provider holds the DI-resolved clients and caches ARNs/URLs (singleton).
        builder.Services.AddSingleton<SnsSqsClientProvider>();

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

    /// <summary>
    /// Creates an AWS service client of type <typeparamref name="TClient"/>, inheriting
    /// credentials and region from the DI-registered <see cref="AWSOptions"/> (if any) and
    /// overriding the service endpoint URL. Used when a custom <c>ServiceUrl</c> is
    /// configured (e.g. for LocalStack).
    /// </summary>
    private static TClient CreateServiceClientWithServiceUrl<TClient>(
        IServiceProvider sp,
        string serviceUrl)
        where TClient : class, Amazon.Runtime.IAmazonService
    {
        // Inherit credentials/region from globally-registered AWSOptions, then apply
        // the service endpoint override so LocalStack (or other custom endpoints) is targeted.
        var baseOptions = sp.GetService<AWSOptions>() ?? new AWSOptions();
        var overrideOptions = new AWSOptions
        {
            Credentials = baseOptions.Credentials,
            Region = baseOptions.Region,
            Profile = baseOptions.Profile,
            ProfilesLocation = baseOptions.ProfilesLocation,
        };
        overrideOptions.DefaultClientConfig.ServiceURL = serviceUrl;
        return overrideOptions.CreateServiceClient<TClient>();
    }
}
