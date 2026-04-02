using InterBus.Extensions;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for the <see cref="IScheduleMiddleware"/> pipeline wired into
/// <see cref="MessageScheduler"/> via <see cref="IInterBusBuilder.UseScheduleMiddleware{TMiddleware}"/>.
/// </summary>
public sealed class ScheduleMiddlewareTests
{
    #region Helpers

    private static (IMessageScheduler Scheduler, TestSchedulingProvider Provider, RecordingScheduleMiddleware Middleware)
        BuildScheduler(Action<IInterBusBuilder>? configureBus = null)
    {
        var schedulingProvider = new TestSchedulingProvider();
        var middleware = new RecordingScheduleMiddleware();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IMessageSchedulingProvider>(schedulingProvider);
            // Register the middleware instance directly so the same instance is injected.
            bus.Services.AddScoped<IScheduleMiddleware>(_ => middleware);
            bus.AddScheduling();
            configureBus?.Invoke(bus);
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        return (scheduler, schedulingProvider, middleware);
    }

    #endregion

    [Fact]
    public async Task ScheduleMiddleware_IsInvoked_OnSchedulePublish()
    {
        var (scheduler, _, middleware) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), scheduledAt,
            TestContext.Current.CancellationToken);

        Assert.Single(middleware.Contexts);
        Assert.Equal(scheduledAt, middleware.Contexts[0].ScheduledAt);
    }

    [Fact]
    public async Task ScheduleMiddleware_IsInvoked_OnScheduleSend_ByConsumerType()
    {
        var (scheduler, _, middleware) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);

        await scheduler.ScheduleSend<TestConsumer, TestMessage>(new TestMessage(Guid.NewGuid()), scheduledAt,
            TestContext.Current.CancellationToken);

        Assert.Single(middleware.Contexts);
        Assert.Equal("queue", middleware.Contexts[0].Destination.Scheme);
        Assert.Equal(scheduledAt, middleware.Contexts[0].ScheduledAt);
    }

    [Fact]
    public async Task ScheduleMiddleware_IsInvoked_OnScheduleSend_ByQueueName()
    {
        var (scheduler, _, middleware) = BuildScheduler();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(1);

        await scheduler.ScheduleSend("my-queue", new TestMessage(Guid.NewGuid()), scheduledAt,
            TestContext.Current.CancellationToken);

        Assert.Single(middleware.Contexts);
        Assert.Equal("my-queue", RoutingConventions.GetDestinationName(middleware.Contexts[0].Destination));
        Assert.Equal(scheduledAt, middleware.Contexts[0].ScheduledAt);
    }

    [Fact]
    public async Task ScheduleMiddleware_ReceivesSerializedPayload()
    {
        var (scheduler, _, middleware) = BuildScheduler();

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), DateTimeOffset.UtcNow.AddSeconds(30),
            TestContext.Current.CancellationToken);

        var ctx = Assert.Single(middleware.Contexts);
        Assert.NotEmpty(ctx.Payload);
    }

    [Fact]
    public async Task ScheduleMiddleware_ReceivesMessageTypeHeader()
    {
        var (scheduler, _, middleware) = BuildScheduler();

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), DateTimeOffset.UtcNow.AddSeconds(10),
            TestContext.Current.CancellationToken);

        var ctx = Assert.Single(middleware.Contexts);
        Assert.True(ctx.Headers.ContainsKey(InterBusHeaders.MessageType));
    }

    [Fact]
    public async Task ScheduleMiddleware_CanMutatePayload_AndProviderReceivesMutated()
    {
        var schedulingProvider = new TestSchedulingProvider();
        var replacedPayload = new byte[] { 0xFF, 0xFE };
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IMessageSchedulingProvider>(schedulingProvider);
            bus.Services.AddScoped<IScheduleMiddleware>(_ => new PayloadReplacingScheduleMiddleware(replacedPayload));
            bus.AddScheduling();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), DateTimeOffset.UtcNow.AddMinutes(1),
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal(replacedPayload, call.Payload);
    }

    [Fact]
    public async Task ScheduleMiddleware_CanMutateHeaders_AndProviderReceivesMutated()
    {
        var schedulingProvider = new TestSchedulingProvider();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IMessageSchedulingProvider>(schedulingProvider);
            bus.Services.AddScoped<IScheduleMiddleware, HeaderAddingScheduleMiddleware>();
            bus.AddScheduling();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), DateTimeOffset.UtcNow.AddMinutes(1),
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.True(call.Headers.ContainsKey("x-added-by-middleware"));
    }

    [Fact]
    public async Task MultipleScheduleMiddlewares_ExecuteInRegistrationOrder()
    {
        var order = new List<string>();
        var schedulingProvider = new TestSchedulingProvider();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IMessageSchedulingProvider>(schedulingProvider);
            // Register two middlewares in order; use factories to capture the shared list.
            bus.Services.AddScoped<IScheduleMiddleware>(_ => new OrderTrackingScheduleMiddleware("first", order));
            bus.Services.AddScoped<IScheduleMiddleware>(_ => new OrderTrackingScheduleMiddleware("second", order));
            bus.AddScheduling();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), DateTimeOffset.UtcNow.AddMinutes(1),
            TestContext.Current.CancellationToken);

        Assert.Equal(["first", "second"], order);
    }

    [Fact]
    public void UseScheduleMiddleware_RegistersIScheduleMiddleware()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.UseScheduleMiddleware<HeaderAddingScheduleMiddleware>();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var middlewares = scope.ServiceProvider.GetServices<IScheduleMiddleware>().ToList();

        Assert.Single(middlewares);
        Assert.IsType<HeaderAddingScheduleMiddleware>(middlewares[0]);
    }

    [Fact]
    public void UseScheduleMiddleware_IsNotRequired_SchedulerStillWorksWithoutIt()
    {
        var schedulingProvider = new TestSchedulingProvider();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IMessageSchedulingProvider>(schedulingProvider);
            bus.AddScheduling();
            // No schedule middleware registered - should work fine
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        Assert.NotNull(scheduler);
    }

    [Fact]
    public async Task ScheduleMiddleware_IsNotInvoked_OnRegularPublish()
    {
        var schedulingProvider = new TestSchedulingProvider();
        var middleware = new RecordingScheduleMiddleware();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IMessageSchedulingProvider>(schedulingProvider);
            bus.Services.AddScoped<IScheduleMiddleware>(_ => middleware);
            bus.AddScheduling();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

        await publisher.Publish(new TestMessage(Guid.NewGuid()), TestContext.Current.CancellationToken);

        Assert.Empty(middleware.Contexts);
    }

    #region Support types

    private sealed record TestMessage(Guid Id);

    private sealed class TestConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class NoOpTransport : IInterBusTransport
    {
        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class TestSchedulingProvider : IMessageSchedulingProvider
    {
        public List<SchedulingCall> Calls { get; } = [];

        public Task ScheduleAsync(Uri destination, byte[] payload, MessageHeaders headers,
            DateTimeOffset scheduledAt, CancellationToken cancellationToken)
        {
            Calls.Add(new SchedulingCall(destination, payload, headers, scheduledAt));
            return Task.CompletedTask;
        }
    }

    private sealed record SchedulingCall(
        Uri Destination,
        byte[] Payload,
        MessageHeaders Headers,
        DateTimeOffset ScheduledAt);

    /// <summary>Records each <see cref="ScheduleContext"/> it receives.</summary>
    private sealed class RecordingScheduleMiddleware : IScheduleMiddleware
    {
        public List<ScheduleContext> Contexts { get; } = [];

        public Task InvokeAsync(ScheduleContext context, Func<ScheduleContext, CancellationToken, Task> next,
            CancellationToken cancellationToken = default)
        {
            Contexts.Add(context);
            return next(context, cancellationToken);
        }
    }

    /// <summary>Replaces the payload with a fixed byte array.</summary>
    private sealed class PayloadReplacingScheduleMiddleware(byte[] replacement) : IScheduleMiddleware
    {
        public Task InvokeAsync(ScheduleContext context, Func<ScheduleContext, CancellationToken, Task> next,
            CancellationToken cancellationToken = default)
        {
            context.Payload = replacement;
            return next(context, cancellationToken);
        }
    }

    /// <summary>Adds a header to the context.</summary>
    private sealed class HeaderAddingScheduleMiddleware : IScheduleMiddleware
    {
        public Task InvokeAsync(ScheduleContext context, Func<ScheduleContext, CancellationToken, Task> next,
            CancellationToken cancellationToken = default)
        {
            context.Headers = new MessageHeaders(context.Headers) { ["x-added-by-middleware"] = "true" };
            return next(context, cancellationToken);
        }
    }

    private sealed class OrderTrackingScheduleMiddleware(string name, List<string> order) : IScheduleMiddleware
    {
        public Task InvokeAsync(ScheduleContext context, Func<ScheduleContext, CancellationToken, Task> next,
            CancellationToken cancellationToken = default)
        {
            order.Add(name);
            return next(context, cancellationToken);
        }
    }

    #endregion
}

