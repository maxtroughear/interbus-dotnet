namespace InterBus.EntityFrameworkCore.Tests;

public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);
