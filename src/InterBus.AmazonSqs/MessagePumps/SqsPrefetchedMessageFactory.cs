using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace InterBus.AmazonSqs.MessagePumps;

public sealed class SqsPrefetchedMessageFactory(
    ILoggerFactory loggerFactory,
    IAmazonSQS sqsClient,
    SnsSqsTransportOptions options)
{
    public SqsPrefetchedMessage Create(Message message, string queueUrl, string deadLetterQueueUrl, CancellationToken stoppingToken)
    {
        return new SqsPrefetchedMessage(
            loggerFactory.CreateLogger<SqsPrefetchedMessage>(),
            sqsClient,
            options,
            message,
            queueUrl,
            deadLetterQueueUrl,
            stoppingToken);
    }
}
