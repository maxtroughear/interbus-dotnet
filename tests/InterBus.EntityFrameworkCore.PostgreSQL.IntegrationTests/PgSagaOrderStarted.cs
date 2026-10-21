namespace InterBus.EntityFrameworkCore.PostgreSQL.IntegrationTests;

public record PgSagaOrderStarted(Guid OrderId, string CustomerEmail);
