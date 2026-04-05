namespace InterBus.EntityFrameworkCore.PostgreSQL.IntegrationTests;

public record OrderPlaced(Guid OrderId, string CustomerEmail, decimal Total);
