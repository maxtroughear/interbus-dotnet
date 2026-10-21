namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public record SqlSagaOrderStarted(Guid OrderId, string CustomerEmail);
