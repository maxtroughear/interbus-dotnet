namespace InterBus.EntityFrameworkCore.Tests;

public record EfSagaOrderStarted(Guid OrderId, string CustomerEmail);
