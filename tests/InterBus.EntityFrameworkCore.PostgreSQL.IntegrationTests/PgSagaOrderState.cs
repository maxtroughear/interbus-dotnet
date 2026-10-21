namespace InterBus.EntityFrameworkCore.PostgreSQL.IntegrationTests;

public sealed class PgSagaOrderState
{
    public string? CustomerEmail { get; set; }
    public bool IsConfirmed { get; set; }
    public int MessageCount { get; set; }
}
