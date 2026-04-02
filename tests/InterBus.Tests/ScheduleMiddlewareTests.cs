using InterBus.Extensions;
using InterBus.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for the <see cref="IScheduleMiddleware"/> pipeline executed by
/// <see cref="SchedulePipelineExecutor"/> at message delivery time.
/// </summary>
public sealed class ScheduleMiddlewareTests
{
    #region SchedulePipelineExecutor tests

    [Fact]
    public async Task SchedulePipelineExecutor_InvokesMiddleware_BeforeTransport()
    {
        var transport = new RecordingTransport();
        var middleware = new RecordingScheduleMiddleware();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport>(transport);
            bus.Services.AddScoped<IScheduleMiddleware>(_ => middleware);
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var executor = scope.ServiceProvider.GetRequiredService<SchedulePipelineExecutor>();
        var destination = new Uri("topic://interbus/test-topic");
        var ctx = new ScheduleContext(destination, [1, 2, 3], new MessageHeaders());

        await executor.ExecuteAsync(ctx, TestContext.Current.CancellationToken);

        Assert.Single(middleware.Contexts);
        Assert.Single(transport.Sent);
    }

    [Fact]
    public async Task SchedulePipelineExecutor_ForwardsToTransport_WhenNoMiddleware()
    {
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport>(transport);
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var executor = scope.ServiceProvider.GetRequiredService<SchedulePipelineExecutor>();
        var destination = new Uri("topic://interbus/test-topic");
        var ctx = new ScheduleContext(destination, [1, 2, 3], new MessageHeaders());

        await executor.ExecuteAsync(ctx, TestContext.Current.CancellationToken);

        Assert.Single(transport.Sent);
        Assert.Equal([1, 2, 3], transport.Sent[0].Payload);
    }

    [Fact]
    public async Task SchedulePipelineExecutor_CanMutatePayload_AndTransportReceivesMutated()
    {
        var transport = new RecordingTransport();
        var replacedPayload = new byte[] { 0xFF, 0xFE };
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport>(transport);
            bus.Services.AddScoped<IScheduleMiddleware>(_ => new PayloadReplacingScheduleMiddleware(replacedPayload));
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var executor = scope.ServiceProvider.GetRequiredService<SchedulePipelineExecutor>();
        var ctx = new ScheduleContext(new Uri("queue://interbus/my-queue"), [1, 2, 3], new MessageHeaders());

        await executor.ExecuteAsync(ctx, TestContext.Current.CancellationToken);

        Assert.Single(transport.Sent);
        Assert.Equal(replacedPayload, transport.Sent[0].Payload);
    }

    [Fact]
    public async Task SchedulePipelineExecutor_CanMutateHeaders_AndTransportReceivesMutated()
    {
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport>(transport);
            bus.Services.AddScoped<IScheduleMiddleware, HeaderAddingScheduleMiddleware>();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var executor = scope.ServiceProvider.GetRequiredService<SchedulePipelineExecutor>();
        var ctx = new ScheduleContext(new Uri("queue://interbus/my-queue"), [1], new MessageHeaders());

        await executor.ExecuteAsync(ctx, TestContext.Current.CancellationToken);

        Assert.Single(transport.Sent);
        Assert.True(transport.Sent[0].Headers.ContainsKey("x-added-by-middleware"));
    }

    [Fact]
    public async Task SchedulePipelineExecutor_MultipleMiddlewares_ExecuteInRegistrationOrder()
    {
        var order = new List<string>();
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport>(transport);
            bus.Services.AddScoped<IScheduleMiddleware>(_ => new OrderTrackingScheduleMiddleware("first", order));
            bus.Services.AddScoped<IScheduleMiddleware>(_ => new OrderTrackingScheduleMiddleware("second", order));
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var executor = scope.ServiceProvider.GetRequiredService<SchedulePipelineExecutor>();
        var ctx = new ScheduleContext(new Uri("queue://interbus/my-queue"), [1], new MessageHeaders());

        await executor.ExecuteAsync(ctx, TestContext.Current.CancellationToken);

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

    #endregion

    #region MessageScheduler tests - no middleware at schedule time

    [Fact]
    public async Task MessageScheduler_DoesNotInvokeScheduleMiddleware_AtScheduleTime()
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
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), DateTimeOffset.UtcNow.AddMinutes(5),
            TestContext.Current.CancellationToken);

        // Middleware should NOT have been invoked at schedule time
        Assert.Empty(middleware.Contexts);
        // But the scheduling provider should have been called
        Assert.Single(schedulingProvider.Calls);
    }

    [Fact]
    public async Task MessageScheduler_CallsProviderDirectly_WithSerializedPayload()
    {
        var schedulingProvider = new TestSchedulingProvider();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus =>
        {
            bus.Services.AddSingleton<IInterBusTransport, NoOpTransport>();
            bus.Services.AddSingleton<IMessageSchedulingProvider>(schedulingProvider);
            bus.AddScheduling();
        });

        var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var scheduler = scope.ServiceProvider.GetRequiredService<IMessageScheduler>();
        var scheduledAt = DateTimeOffset.UtcNow.AddMinutes(5);

        await scheduler.SchedulePublish(new TestMessage(Guid.NewGuid()), scheduledAt,
            TestContext.Current.CancellationToken);

        var call = Assert.Single(schedulingProvider.Calls);
        Assert.Equal("topic", call.Destination.Scheme);
        Assert.Equal(scheduledAt, call.ScheduledAt);
        Assert.NotEmpty(call.Payload);
        Assert.True(call.Headers.ContainsKey(InterBusHeaders.MessageType));
    }

    #endregion

    #region Support types

    private sealed record TestMessage(Guid Id);

    private sealed class NoOpTransport : IInterBusTransport
    {
        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class RecordingTransport : IInterBusTransport
    {
        public List<TransportSendContext> Sent { get; } = [];

        public Task SendAsync(TransportSendContext context, CancellationToken cancellationToken)
        {
            Sent.Add(context);
            return Task.CompletedTask;
        }
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
