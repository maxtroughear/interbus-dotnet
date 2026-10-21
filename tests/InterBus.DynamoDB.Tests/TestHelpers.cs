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
}
