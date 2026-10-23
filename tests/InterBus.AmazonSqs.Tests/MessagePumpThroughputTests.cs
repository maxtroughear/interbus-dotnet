using System.Diagnostics;
using Amazon.SQS.Model;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.AmazonSqs.Tests;

public sealed partial class MessagePumpAdapterTests
{
    [TestCase(0, false)]
    [TestCase(32, false)]
    [TestCase(0, true)]
    [TestCase(32, true)]
    public async Task ReceiveThroughput_WithFixedLatency(int prefetch, bool circuit)
    {
        const int count = 512, concurrency = 8;
        using var client = new FakeSqs { ReceiveLatency = TimeSpan.FromMilliseconds(2), ExpectedDeletes = count };
        var envelope = new SnsSqsMessageEnvelope();
        for (var i = 0; i < count; i++)
            client.Messages.Enqueue(new Message { ReceiptHandle = i.ToString(), Body = envelope.SerializeForSnsPublish([], new MessageHeaders()) });
        long busyTicks = 0;
        await using var provider = Configure(client, async (_, _, _, token) =>
        {
            var start = Stopwatch.GetTimestamp();
            await Task.Delay(5, token);
            Interlocked.Add(ref busyTicks, Stopwatch.GetTimestamp() - start);
        }, concurrency, prefetch);
        var descriptor = provider.GetRequiredService<InterBusOptions>().Consumers.Single();
        if (circuit) new InterBus.Consuming.ConsumerBuilder(descriptor, new ServiceCollection()).UsePumpCircuitBreaker();
        var source = provider.GetRequiredService<IConsumerSourceFactory>().Create(descriptor);
        await using var lifetime = new PumpLifetime(descriptor, source, provider.GetRequiredService<DeliveryProcessor>());
        var watch = Stopwatch.StartNew();
        await lifetime.StartAsync(TestContext.CurrentContext.CancellationToken);
        await client.AllDeleted.Task.WaitAsync(TimeSpan.FromSeconds(15), TestContext.CurrentContext.CancellationToken);
        watch.Stop();
        await lifetime.StopAsync(TestContext.CurrentContext.CancellationToken);
        var measurement = $"circuit={circuit}; prefetch={prefetch}; messages={count}; concurrency={concurrency}; pumps=3; batchLimit=10; elapsedMs={watch.Elapsed.TotalMilliseconds:F1}; receives={client.ReceiveCalls}; maxOwned={client.MaximumOwned}; handlerUtilization={(double)busyTicks / Stopwatch.Frequency / watch.Elapsed.TotalSeconds / concurrency:P1}";
        TestContext.Out.WriteLine(measurement);
        if (Environment.GetEnvironmentVariable("INTERBUS_PUMP_BENCHMARK_OUTPUT") is { } output)
            await File.AppendAllTextAsync(output, measurement + Environment.NewLine, TestContext.CurrentContext.CancellationToken);
        Assert.That(client.MaximumOwned, Is.LessThanOrEqualTo(concurrency + prefetch));
        Assert.That(client.Owned, Is.Zero);
    }
}
