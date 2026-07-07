namespace InterBus.AzureServiceBus.Tests;

public record SessionOrderPlaced(Guid OrderId, string CustomerId, decimal Total);
