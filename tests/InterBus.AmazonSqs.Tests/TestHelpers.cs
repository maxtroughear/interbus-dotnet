using Amazon;
using Amazon.Extensions.NETCore.Setup;
using Amazon.Runtime;
using Testcontainers.LocalStack;

namespace InterBus.AmazonSqs.Tests;

public static class TestHelpers
{
    /// <summary>
    /// When <c>INTERBUS_USE_REAL_AWS=true</c> the tests skip LocalStack and authenticate
    /// via credentials already present in the environment (e.g. OIDC-injected env vars).
    /// </summary>
    internal static bool UseRealAws =>
        string.Equals(
            Environment.GetEnvironmentVariable("INTERBUS_USE_REAL_AWS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>Returns a new LocalStack container, or <c>null</c> when using real AWS.</summary>
    internal static LocalStackContainer? CreateLocalStackContainer() =>
        UseRealAws ? null : new LocalStackBuilder("localstack/localstack:4").Build();

    /// <summary>
    /// Builds <see cref="AWSOptions"/> for either LocalStack or real AWS.
    /// When <paramref name="container"/> is <c>null</c> the SDK credential chain resolves
    /// credentials from the environment (suitable for OIDC / IAM role scenarios).
    /// </summary>
    internal static AWSOptions CreateAwsOptions(LocalStackContainer? container)
    {
        if (container is null)
            return new AWSOptions { Region = RegionEndpoint.USEast1 };

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
