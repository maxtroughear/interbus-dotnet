namespace InterBus.EntityFrameworkCore.Tests;

public sealed class FailSendOnceController
{
    public bool ShouldFail { get; set; } = true;
}
