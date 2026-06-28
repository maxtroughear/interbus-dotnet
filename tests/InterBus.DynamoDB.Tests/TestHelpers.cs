using Amazon;
using Amazon.Extensions.NETCore.Setup;
using Amazon.Runtime;
using Testcontainers.LocalStack;

namespace InterBus.DynamoDB.Tests;

public static class TestHelpers
{
    private static bool UseRealAws =>
        string.Equals(
            Environment.GetEnvironmentVariable("INTERBUS_USE_REAL_AWS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    internal static LocalStackContainer? CreateLocalStackContainer() =>
        UseRealAws ? null : new LocalStackBuilder("ministackorg/ministack:full").Build();

    internal static AWSOptions? CreateAwsOptions(LocalStackContainer? container)
    {
        if (container is null)
            return null;

        return new AWSOptions
        {
            Credentials = new BasicAWSCredentials("test", "test"),
            Region = RegionEndpoint.USEast1,
            DefaultClientConfig = { ServiceURL = container.GetConnectionString() }
        };
    }

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
