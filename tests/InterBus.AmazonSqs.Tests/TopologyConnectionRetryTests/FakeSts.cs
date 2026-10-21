using Amazon;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;

namespace InterBus.AmazonSqs.Tests;

public partial class TopologyConnectionRetryTests
{
    private sealed class FakeSts() : AmazonSecurityTokenServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1)
    {
        public int Attempts;
        public override Task<GetCallerIdentityResponse> GetCallerIdentityAsync(GetCallerIdentityRequest request, CancellationToken cancellationToken = default)
        {
            if (++Attempts == 1)
                throw new AmazonSecurityTokenServiceException("temporary")
                {
                    StatusCode = System.Net.HttpStatusCode.ServiceUnavailable
                };
            return Task.FromResult(new GetCallerIdentityResponse { Account = "123456789012", Arn = "arn:aws:iam::123456789012:user/test", UserId = "test" });
        }
    }
}
