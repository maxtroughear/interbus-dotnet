using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Microsoft.Extensions.Logging.Abstractions;

namespace InterBus.AmazonSqs.Tests;

[TestFixture]
public partial class TopologyConnectionRetryTests
{
    [Test]
    public async Task ResolveOnly_RetriesBeforeConsumption_AndHonorsCancellation()
    {
        using var sqs = new AmazonSQSClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1);
        using var sns = new AmazonSimpleNotificationServiceClient(new AnonymousAWSCredentials(), RegionEndpoint.USEast1);
        using var sts = new FakeSts();
        var options = new SnsSqsTransportOptions { TopologyMode = TopologyMode.ResolveOnly };
        options.ConnectionRetry.DelayProvider = _ => TimeSpan.Zero;
        var bus = new InterBusOptions();
        bus.GetConsumerDescriptor(typeof(TopologyConnectionRetryTests));
        var topology = new SnsSqsTopologyBuilder(sns, sqs, sts, new SnsSqsNaming(options), options, bus,
            NullLogger<SnsSqsTopologyBuilder>.Instance);
        await topology.BuildConsumerTopologyAsync(default);
        Assert.That(sts.Attempts, Is.EqualTo(2));
        Assert.That(topology.QueueUrls.Count, Is.EqualTo(1));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.ThrowsAsync<OperationCanceledException>(() => topology.BuildConsumerTopologyAsync(cancellation.Token));
        Assert.That(sts.Attempts, Is.EqualTo(2));
    }
}
