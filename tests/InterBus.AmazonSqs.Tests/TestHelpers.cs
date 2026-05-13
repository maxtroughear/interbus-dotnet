using Amazon;
using Amazon.Extensions.NETCore.Setup;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

public static class TestHelpers
{
    /// <summary>
    /// When <c>INTERBUS_USE_REAL_AWS=true</c> the tests skip LocalStack and authenticate
    /// via credentials already present in the environment (e.g. OIDC-injected env vars).
    /// </summary>
    internal static bool UseRealAws =>
        string.Equals(
            Environment.GetEnvironmentVariable("INTERBUS_USE_REAL_AWS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    internal static LocalStackContainer? CreateLocalStackContainer() =>
        UseRealAws ? null : new LocalStackBuilder("localstack/localstack:4").Build();

    internal static AWSOptions? CreateAwsOptions(LocalStackContainer? container)
    {
        if (container is null)
            return null;

        return new AWSOptions
        {
            Credentials = new BasicAWSCredentials("test", "test"),
            Region = RegionEndpoint.USEast1,
            DefaultClientConfig = { ServiceURL = container.GetConnectionString() }
        };
    }

    internal static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 30_000,
        int intervalMs = 200,
        CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
        {
            try
            {
                await Task.Delay(intervalMs, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Cleans up AWS resources (SQS queues, SNS subscriptions, and SNS topics)
    /// matching the specified prefix.
    /// This is useful for test cleanup to remove test-specific queues and topics.
    /// </summary>
    internal static async Task CleanupResourcesByPrefixAsync(
        IAmazonSQS sqsClient,
        IAmazonSimpleNotificationService snsClient,
        string queuePrefix,
        string topicPrefix,
        ILogger? logger = null)
    {
        try
        {
            await DeleteQueuesWithPrefixAsync(sqsClient, queuePrefix, logger);

            await DeleteTopicsWithPrefixAsync(snsClient, topicPrefix, logger);
        }
        catch (Exception ex)
        {
            logger?.LogError(ex,
                "Error cleaning up AWS resources with prefixes queue={QueuePrefix}, topic={TopicPrefix}", queuePrefix,
                topicPrefix);
            // Don't rethrow - cleanup failures shouldn't fail tests
        }
    }

    /// <summary>
    /// Deletes all SQS queues whose names start with the specified prefix.
    /// </summary>
    private static async Task DeleteQueuesWithPrefixAsync(
        IAmazonSQS sqsClient,
        string queuePrefix,
        ILogger? logger = null)
    {
        try
        {
            var response = await sqsClient.ListQueuesAsync(new ListQueuesRequest { QueueNamePrefix = queuePrefix });

            if (response.QueueUrls == null || response.QueueUrls.Count == 0)
            {
                logger?.LogDebug("No SQS queues found with prefix {QueuePrefix}", queuePrefix);
                return;
            }

            foreach (var queueUrl in response.QueueUrls)
            {
                try
                {
                    logger?.LogDebug("Deleting SQS queue: {QueueUrl}", queueUrl);
                    await sqsClient.DeleteQueueAsync(queueUrl);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex, "Failed to delete SQS queue {QueueUrl}", queueUrl);
                }
            }
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Error listing SQS queues with prefix {QueuePrefix}", queuePrefix);
        }
    }

    /// <summary>
    /// Deletes all SNS topics whose names start with the specified prefix.
    /// </summary>
    private static async Task DeleteTopicsWithPrefixAsync(
        IAmazonSimpleNotificationService snsClient,
        string topicPrefix,
        ILogger? logger = null)
    {
        try
        {
            string? nextToken = null;
            do
            {
                var response = await snsClient.ListTopicsAsync(nextToken);

                if (response.Topics == null || response.Topics.Count == 0)
                    break;

                var topicsToDelete = response.Topics
                    .Where(t => t.TopicArn.Contains($":{topicPrefix}", StringComparison.Ordinal))
                    .ToList();

                if (topicsToDelete.Count == 0 && string.IsNullOrEmpty(nextToken))
                {
                    logger?.LogDebug("No SNS topics found with prefix {TopicPrefix}", topicPrefix);
                    break;
                }

                foreach (var topic in topicsToDelete)
                {
                    try
                    {
                        await DeleteSubscriptionsByTopicAsync(snsClient, topic.TopicArn, logger);

                        logger?.LogDebug("Deleting SNS topic: {TopicArn}", topic.TopicArn);
                        await snsClient.DeleteTopicAsync(topic.TopicArn);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogWarning(ex, "Failed to delete SNS topic {TopicArn}", topic.TopicArn);
                    }
                }

                nextToken = response.NextToken;
            } while (!string.IsNullOrEmpty(nextToken));
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Error listing SNS topics with prefix {TopicPrefix}", topicPrefix);
        }
    }

    /// <summary>
    /// Deletes all subscriptions for a topic. Pending subscriptions are skipped because
    /// they do not have a real ARN that can be unsubscribed.
    /// </summary>
    private static async Task DeleteSubscriptionsByTopicAsync(
        IAmazonSimpleNotificationService snsClient,
        string topicArn,
        ILogger? logger = null)
    {
        string? nextToken = null;

        do
        {
            ListSubscriptionsByTopicResponse response;
            try
            {
                response = await snsClient.ListSubscriptionsByTopicAsync(
                    new ListSubscriptionsByTopicRequest
                    {
                        TopicArn = topicArn,
                        NextToken = nextToken,
                    });
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "Error listing subscriptions for SNS topic {TopicArn}", topicArn);
                return;
            }

            if (response.Subscriptions == null || response.Subscriptions.Count == 0)
                return;

            foreach (var subscription in response.Subscriptions)
            {
                if (string.Equals(subscription.SubscriptionArn, "PendingConfirmation", StringComparison.Ordinal))
                    continue;

                try
                {
                    logger?.LogDebug("Deleting SNS subscription: {SubscriptionArn}", subscription.SubscriptionArn);
                    await snsClient.UnsubscribeAsync(
                        new UnsubscribeRequest { SubscriptionArn = subscription.SubscriptionArn });
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(
                        ex,
                        "Failed to delete SNS subscription {SubscriptionArn} for topic {TopicArn}",
                        subscription.SubscriptionArn,
                        topicArn);
                }
            }

            nextToken = response.NextToken;
        } while (!string.IsNullOrEmpty(nextToken));
    }
}
