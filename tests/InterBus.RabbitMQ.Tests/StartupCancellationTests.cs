using System.Reflection;
using InterBus.Consuming;
using InterBus.Extensions;
using InterBus.MessagePumps;
using InterBus.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace InterBus.RabbitMQ.Tests;

[TestFixture]
public class StartupCancellationTests
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task SubscriptionCancellation_IsExpectedOnlyDuringStartup(bool startup)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus => bus.AddRabbitMqTransport());
        await using var provider = services.BuildServiceProvider();
        var descriptor = new ConsumerDescriptor(typeof(StartupCancellationTests), new Uri("rabbitmqqueue://startup"));
        await using var source = (ICallbackSource)provider.GetRequiredService<IConsumerSourceFactory>().Create(descriptor);
        var channel = DispatchProxy.Create<IChannel, StartupChannel>();
        var proxy = (StartupChannel)(object)channel;
        source.GetType().GetField("_channel", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(source, channel);
        if (!startup) await source.StartAsync((_, _) => Task.CompletedTask, default, default);
        proxy.Block = true;
        using var operation = new CancellationTokenSource();
        var subscribe = startup
            ? source.StartAsync((_, _) => Task.CompletedTask, default, operation.Token)
            : source.ResumeAsync(operation.Token);
        await proxy.Entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        operation.Cancel();
        await Assert.CatchAsync<OperationCanceledException>(async () => await subscribe);
        Assert.That(source.Failure.IsCompleted, Is.EqualTo(!startup));
        await source.PauseAsync(default);
    }

    public class StartupChannel : DispatchProxy
    {
        public bool Block;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        protected override object? Invoke(MethodInfo? method, object?[]? args) => method!.Name switch
        {
            nameof(IChannel.BasicConsumeAsync) => SubscribeAsync(args!.OfType<CancellationToken>().Single()),
            nameof(IAsyncDisposable.DisposeAsync) => ValueTask.CompletedTask,
            _ => throw new NotSupportedException(method.Name)
        };
        private async Task<string> SubscribeAsync(CancellationToken token)
        {
            if (Block)
            {
                Entered.TrySetResult();
                await Task.Delay(Timeout.Infinite, token);
            }
            return "test-consumer";
        }
    }
}
