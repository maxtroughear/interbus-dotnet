namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);
