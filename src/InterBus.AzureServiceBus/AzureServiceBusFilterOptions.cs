using Azure.Messaging.ServiceBus.Administration;

namespace InterBus.AzureServiceBus;

internal class AzureServiceBusFilterOptions
{
    public Dictionary<(Type ConsumerType, Type MessageType), SqlRuleFilter> Filters { get; } = new();
}
