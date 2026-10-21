namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public sealed class SqlSagaOrderState
{
    public string? CustomerEmail { get; set; }
    public bool IsConfirmed { get; set; }
    public int MessageCount { get; set; }
}
