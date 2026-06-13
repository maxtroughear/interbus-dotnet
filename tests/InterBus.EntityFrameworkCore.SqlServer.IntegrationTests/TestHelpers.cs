namespace InterBus.EntityFrameworkCore.SqlServer.IntegrationTests;

public static class TestHelpers
{
    internal static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 100_000,
        int intervalMs = 50)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }
}
