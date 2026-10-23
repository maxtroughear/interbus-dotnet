using Azure.Messaging.ServiceBus;

namespace InterBus.AzureServiceBus;

internal static class AzureServiceBusMessageFactory
{
    public static ServiceBusMessage Create(byte[] payload, MessageHeaders headers, string? virtualQueueId = null)
    {
        var message = new ServiceBusMessage(payload);
        foreach (var header in headers)
            message.ApplicationProperties[header.Key] = header.Value;
        if (virtualQueueId is not null)
            message.SessionId = virtualQueueId;
        else if (headers.TryGetValue(InterBusHeaders.MessageGroupId, out var groupId))
            message.SessionId = groupId;
        return message;
    }
}
