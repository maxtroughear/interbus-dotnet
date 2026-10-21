namespace InterBus.Tests.Common;

public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);
