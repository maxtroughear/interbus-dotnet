using Amazon.SQS;
using Amazon.SQS.Model;
using InterBus.Requests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs;

/// <summary>
/// Amazon SQS implementation of <see cref="IReplyEndpointFactory"/>.
/// Creates a short-lived, single-use SQS queue for each request/reply exchange.
/// </summary>
internal sealed class SqsReplyEndpointFactory(
    IAmazonSQS sqsClient,
    SnsSqsTopology topology,
    SnsSqsTransportOptions options,
    ISnsSqsMessageEnvelope envelope,
    IServiceScopeFactory scopeFactory,
    ILogger<SqsReplyEndpoint> logger)
    : IReplyEndpointFactory
{
    public async Task<IReplyEndpoint> CreateAsync(CancellationToken cancellationToken = default)
    {
        var queueName = $"interbus-reply-{Guid.CreateVersion7():N}";

        var attributes = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            // Keep messages for 5 minutes; reply exchanges are short-lived.
            ["MessageRetentionPeriod"] = "300",
        };

        if (options.EnableSqsEncryption)
            attributes["SqsManagedSseEnabled"] = "true";

        var response = await sqsClient.CreateQueueAsync(
            new CreateQueueRequest { QueueName = queueName, Attributes = attributes },
            cancellationToken);

        var queueUrl = response.QueueUrl;

        // Register in topology cache so SqsTransport.ResolveQueueUrlAsync finds this queue
        // by name when the consumer sends the reply back.
        topology.QueueUrls.TryAdd(queueName, queueUrl);

        return new SqsReplyEndpoint(sqsClient, topology, envelope, scopeFactory, logger, queueName, queueUrl);
    }
}
