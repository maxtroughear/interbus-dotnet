namespace InterBus.AmazonSqs.Tests;

public static class TestHelpers
{
    internal static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 30_000,
        int intervalMs = 200)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(intervalMs);
    }
}
