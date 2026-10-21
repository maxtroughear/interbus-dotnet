namespace InterBus.DynamoDB.Tests;

public sealed class OrderSagaState
{
    public string? CustomerEmail { get; set; }
    public bool IsConfirmed { get; set; }
    public int MessageCount { get; set; }
}
