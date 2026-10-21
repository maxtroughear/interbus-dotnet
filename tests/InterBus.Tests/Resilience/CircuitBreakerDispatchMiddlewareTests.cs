using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests.Resilience;

public sealed partial class CircuitBreakerDispatchMiddlewareTests
{
    [Fact]
    public async Task CircuitBreaker_DoesNotTrip_BeforeMinimumThroughput()
    {
        var descriptor = new ConsumerDescriptor(typeof(TestConsumer), RoutingConventions.QueueUri("queue", "test"));
        await using var provider = BuildProvider(descriptor, options =>
        {
            options.MinimumThroughput = 3;
            options.FailureThreshold = 0.1;
        });
        await using var scope = provider.CreateAsyncScope();
        var middleware = scope.ServiceProvider.GetRequiredKeyedService<IDispatchMiddleware>(descriptor.ConsumerType);

        var nextCallCount = 0;

        for (var i = 0; i < 2; i++)
        {
            var context = new DispatchContext(descriptor, [], new MessageHeaders());
            await middleware.InvokeAsync(
                context,
                (ctx, _) =>
                {
                    nextCallCount++;
                    ctx.Result = DispatchResult.Release(new InvalidOperationException("boom"));
                    return Task.CompletedTask;
                },
                TestContext.Current.CancellationToken);
        }

        var thirdContext = new DispatchContext(descriptor, [], new MessageHeaders());
        await middleware.InvokeAsync(
            thirdContext,
            (_, _) =>
            {
                nextCallCount++;
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(3, nextCallCount);
        Assert.Equal(DispatchResultKind.Success, thirdContext.Result.Kind);
    }

    [Fact]
    public async Task CircuitBreaker_Trips_WhenFailurePercentageExceedsThreshold_AndThenShortCircuits()
    {
        var descriptor = new ConsumerDescriptor(typeof(TestConsumer), RoutingConventions.QueueUri("queue", "test"));
        await using var provider = BuildProvider(descriptor, options =>
        {
            options.MinimumThroughput = 4;
            options.FailureThreshold = 0.5;
            options.BreakDuration = TimeSpan.FromMinutes(1);
        });
        await using var scope = provider.CreateAsyncScope();
        var middleware = scope.ServiceProvider.GetRequiredKeyedService<IDispatchMiddleware>(descriptor.ConsumerType);

        var nextCallCount = 0;

        await InvokeWithResultAsync(middleware, descriptor, DispatchResult.Success, () => nextCallCount++);
        await InvokeWithResultAsync(middleware, descriptor,
            DispatchResult.Release(new InvalidOperationException("boom")),
            () => nextCallCount++);
        await InvokeWithResultAsync(middleware, descriptor, DispatchResult.Success, () => nextCallCount++);
        await InvokeWithResultAsync(middleware, descriptor,
            DispatchResult.Release(new InvalidOperationException("boom")),
            () => nextCallCount++);

        // Exactly 50% failures must not trip the circuit (threshold must be exceeded).
        await InvokeWithResultAsync(middleware, descriptor, DispatchResult.Success, () => nextCallCount++);

        await InvokeWithResultAsync(
            middleware,
            descriptor,
            DispatchResult.Release(new InvalidOperationException("boom")),
            () => nextCallCount++);

        var trippingContext = await InvokeWithResultAsync(
            middleware,
            descriptor,
            DispatchResult.Release(new InvalidOperationException("boom")),
            () => nextCallCount++);

        Assert.Equal(DispatchResultKind.Release, trippingContext.Result.Kind);

        var openContext = new DispatchContext(descriptor, [], new MessageHeaders());
        await middleware.InvokeAsync(
            openContext,
            (_, _) =>
            {
                nextCallCount++;
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(7, nextCallCount);
        Assert.Equal(DispatchResultKind.Release, openContext.Result.Kind);
        Assert.IsType<InvalidOperationException>(openContext.Result.Exception);
        Assert.Contains("temporarily open", openContext.Result.Exception!.Message);
    }

    [Fact]
    public void UseCircuitBreakerMiddleware_ThrowsForInvalidOptions()
    {
        var descriptor = new ConsumerDescriptor(typeof(TestConsumer), RoutingConventions.QueueUri("queue", "test"));
        var services = new ServiceCollection();
        var builder = new ConsumerBuilder(descriptor, services);

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            builder.UseCircuitBreakerMiddleware(options => options.MinimumThroughput = 0));

        Assert.Equal("MinimumThroughput", ex.ParamName);
    }

    private static async Task<DispatchContext> InvokeWithResultAsync(
        IDispatchMiddleware middleware,
        ConsumerDescriptor descriptor,
        DispatchResult result,
        Action onNext)
    {
        var context = new DispatchContext(descriptor, [], new MessageHeaders());
        await middleware.InvokeAsync(
            context,
            (ctx, _) =>
            {
                onNext();
                ctx.Result = result;
                return Task.CompletedTask;
            },
            TestContext.Current.CancellationToken);
        return context;
    }

    private static ServiceProvider BuildProvider(ConsumerDescriptor descriptor, Action<CircuitBreakerOptions> configure)
    {
        var services = new ServiceCollection();
        var builder = new ConsumerBuilder(descriptor, services);
        builder.UseCircuitBreakerMiddleware(configure);
        return services.BuildServiceProvider();
    }
}
