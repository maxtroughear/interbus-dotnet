namespace InterBus.Tests.Common;

public static class TestHelpers
{
    public static async Task WaitUntilAsync(
        Func<bool> condition,
        int timeoutMs = 30_000,
        int intervalMs = 100,
        CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);

        while (!condition() && DateTime.UtcNow < deadline)
        {
            try
            {
                await Task.Delay(intervalMs, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public static async Task WaitUntilAsync(
        Func<Task<bool>> condition,
        int timeoutMs = 30_000,
        int intervalMs = 100,
        CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);

        while (!await condition() && DateTime.UtcNow < deadline)
        {
            try
            {
                await Task.Delay(intervalMs, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}
