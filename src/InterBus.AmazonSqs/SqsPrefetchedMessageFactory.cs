using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs;

public sealed class SqsPrefetchedMessageFactory(
    ILoggerFactory loggerFactory,
    IAmazonSQS sqsClient,
    SnsSqsTransportOptions options)
{
    public SqsPrefetchedMessage Create(Message message, string queueUrl, CancellationToken stoppingToken)
    {
        return new SqsPrefetchedMessage(
            loggerFactory.CreateLogger<SqsPrefetchedMessage>(),
            sqsClient,
            options,
            message,
            queueUrl,
            stoppingToken);
    }
}
