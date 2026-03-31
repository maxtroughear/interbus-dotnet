using InterBus.Middleware;
using InterBus.Retry;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace InterBus.Tests;

/// <summary>
/// Unit tests for <see cref="RetryConsumeMiddleware"/>, <see cref="RetryOptions"/>,
/// and <see cref="RetryDelays"/>.
/// </summary>
public sealed class RetryMiddlewareTests
{
    #region Helpers

    /// <summary>
    /// Builds a minimal <see cref="ConsumeContext"/> using a no-op dispatch descriptor.
    /// </summary>
    private static ConsumeContext<TestMessage> BuildContext() =>
        BuildContext<TestMessage>(new ConsumerDescriptor(
            typeof(NoOpConsumer),
            "test-queue",
            [typeof(TestMessage)],
            (_, _, _) => Task.CompletedTask));

    private static ConsumeContext<T> BuildContext<T>(ConsumerDescriptor descriptor) where T : new() =>
        TestConsumeContextFactory.Create<T>(descriptor);

    /// <summary>
    /// Creates a <see cref="RetryConsumeMiddleware"/> using the given options.
    /// </summary>
    private static RetryConsumeMiddleware CreateMiddleware(RetryOptions options) =>
        new(options, NullLogger<RetryConsumeMiddleware>.Instance);

    #endregion

    #region Retry count

    [Fact]
    public async Task DoesNotRetry_WhenNextSucceeds()
    {
        var callCount = 0;
        var middleware = CreateMiddleware(new RetryOptions { RetryCount = 3 });

        await middleware.InvokeAsync(BuildContext(), (_, _) =>
        {
            callCount++;
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);

        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task RetriesUpToRetryCount_ThenRethrows()
    {
        const int retryCount = 3;
        var callCount = 0;
        var middleware = CreateMiddleware(new RetryOptions { RetryCount = retryCount });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
            {
                callCount++;
                throw new InvalidOperationException("boom");
            }, TestContext.Current.CancellationToken));

        Assert.Equal("boom", ex.Message);
        // Initial attempt + retryCount retries.
        Assert.Equal(retryCount + 1, callCount);
    }

    [Fact]
    public async Task Succeeds_AfterInitialFailure()
    {
        var callCount = 0;
        var middleware = CreateMiddleware(new RetryOptions { RetryCount = 3 });

        await middleware.InvokeAsync(BuildContext(), (_, _) =>
        {
            callCount++;
            if (callCount < 3)
                throw new InvalidOperationException("transient");
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);

        Assert.Equal(3, callCount);
    }

    #endregion

    #region OperationCanceledException

    [Fact]
    public async Task DoesNotRetry_OnOperationCanceledException_WhenTokenCancelled()
    {
        var callCount = 0;
        var middleware = CreateMiddleware(new RetryOptions { RetryCount = 5 });
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
            {
                callCount++;
                throw new OperationCanceledException(cts.Token);
            }, cts.Token));

        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task DoesNotRetry_OnTaskCanceledException_WhenTokenCancelled()
    {
        var callCount = 0;
        var middleware = CreateMiddleware(new RetryOptions { RetryCount = 5 });
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
            {
                callCount++;
                throw new TaskCanceledException();
            }, cts.Token));

        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task Retries_OnOperationCanceledException_FromInternalToken()
    {
        // Consumer uses its own internal CancellationToken (not the middleware's token)
        // The middleware's token is NOT cancelled, so the exception should be retried
        const int retryCount = 2;
        var callCount = 0;
        var middleware = CreateMiddleware(new RetryOptions { RetryCount = retryCount });
        using var internalCts = new CancellationTokenSource();
        await internalCts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
            {
                callCount++;
                throw new OperationCanceledException(internalCts.Token);
            }, TestContext.Current.CancellationToken));

        // Initial attempt + retryCount retries
        Assert.Equal(retryCount + 1, callCount);
    }

    #endregion

    #region RetryOptions.RetryCount validation

    [Fact]
    public void RetryOptions_RetryCount_ThrowsOnZero()
    {
        var opts = new RetryOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.RetryCount = 0);
    }

    [Fact]
    public void RetryOptions_RetryCount_ThrowsOnNegative()
    {
        var opts = new RetryOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => opts.RetryCount = -1);
    }

    #endregion

    #region Exception filtering

    [Fact]
    public async Task ShouldRetry_Predicate_BlocksRetryForNonMatchingException()
    {
        var callCount = 0;
        var opts = new RetryOptions
        {
            RetryCount = 3,
            // Only retry InvalidOperationException, not ArgumentException.
            ShouldRetry = ex => ex is InvalidOperationException
        };
        var middleware = CreateMiddleware(opts);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
            {
                callCount++;
                throw new ArgumentException("not retried");
            }, TestContext.Current.CancellationToken));

        // Only the initial attempt; no retries.
        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task ShouldRetry_Predicate_AllowsRetryForMatchingException()
    {
        var callCount = 0;
        var opts = new RetryOptions
        {
            RetryCount = 2,
            ShouldRetry = ex => ex is InvalidOperationException
        };
        var middleware = CreateMiddleware(opts);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
            {
                callCount++;
                throw new InvalidOperationException("retried");
            }, TestContext.Current.CancellationToken));

        // Initial attempt + 2 retries.
        Assert.Equal(3, callCount);
    }

    #endregion

    #region Delay strategies

    [Fact]
    public void RetryDelays_None_ReturnsZero()
    {
        Assert.Equal(TimeSpan.Zero, RetryDelays.None(1));
        Assert.Equal(TimeSpan.Zero, RetryDelays.None(99));
    }

    [Fact]
    public void RetryDelays_Constant_ReturnsConfiguredDelay()
    {
        var delay = TimeSpan.FromMilliseconds(100);
        var factory = RetryDelays.Constant(delay);

        Assert.Equal(delay, factory(1));
        Assert.Equal(delay, factory(5));
    }

    [Fact]
    public void RetryDelays_Exponential_DoublesEachAttempt()
    {
        var baseDelay = TimeSpan.FromMilliseconds(100);
        var factory = RetryDelays.Exponential(baseDelay);

        // attempt 1 = base * 2^0 = 100ms
        Assert.Equal(TimeSpan.FromMilliseconds(100), factory(1));
        // attempt 2 = base * 2^1 = 200ms
        Assert.Equal(TimeSpan.FromMilliseconds(200), factory(2));
        // attempt 3 = base * 2^2 = 400ms
        Assert.Equal(TimeSpan.FromMilliseconds(400), factory(3));
    }

    [Fact]
    public void RetryDelays_Exponential_RespectsMaxDelay()
    {
        var baseDelay = TimeSpan.FromMilliseconds(100);
        var maxDelay = TimeSpan.FromMilliseconds(300);
        var factory = RetryDelays.Exponential(baseDelay, maxDelay);

        Assert.Equal(TimeSpan.FromMilliseconds(100), factory(1)); // 100 ≤ 300
        Assert.Equal(TimeSpan.FromMilliseconds(200), factory(2)); // 200 ≤ 300
        Assert.Equal(maxDelay, factory(3)); // 400 capped to 300
        Assert.Equal(maxDelay, factory(10)); // all further attempts capped
    }

    [Fact]
    public void RetryDelays_Exponential_DoesNotOverflow_ForLargeAttemptNumbers()
    {
        var factory = RetryDelays.Exponential(TimeSpan.FromSeconds(1));
        // Should not throw for large attempt numbers.
        var result = factory(100);
        Assert.True(result > TimeSpan.Zero);
    }

    [Fact]
    public void RetryDelays_Linear_ReturnsLinearlyIncreasingDelay()
    {
        var baseDelay = TimeSpan.FromMilliseconds(100);
        var factory = RetryDelays.Linear(baseDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(100), factory(1));
        Assert.Equal(TimeSpan.FromMilliseconds(200), factory(2));
        Assert.Equal(TimeSpan.FromMilliseconds(300), factory(3));
    }
    
    [Fact]
    public void RetryDelays_Intervals_ReturnsSpecifiedIntervals()
    {
        var intervals = new[] { TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(300) };
        var factory = RetryDelays.Intervals(intervals);
        Assert.Equal(TimeSpan.FromMilliseconds(100), factory(1));
        Assert.Equal(TimeSpan.FromMilliseconds(200), factory(2));
        Assert.Equal(TimeSpan.FromMilliseconds(300), factory(3));
    }

    [Fact]
    public void RetryDelays_Intervals_ReturnsMaximumInterval_WhenAttemptCountExceedsIntervals()
    {
        var intervals = new[] { TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(300) };
        var factory = RetryDelays.Intervals(intervals);
        Assert.Equal(TimeSpan.FromMilliseconds(300), factory(4));
    }

    #endregion

    #region Delay is applied

    [Fact]
    public async Task Constant_Delay_IsAppliedBetweenRetries()
    {
        var delay = TimeSpan.FromMilliseconds(50);
        var opts = new RetryOptions
        {
            RetryCount = 2,
            DelayProvider = RetryDelays.Constant(delay)
        };
        var middleware = CreateMiddleware(opts);

        var callCount = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            middleware.InvokeAsync(BuildContext(), (_, _) =>
            {
                callCount++;
                throw new InvalidOperationException();
            }, TestContext.Current.CancellationToken));

        sw.Stop();

        // 2 retries × 50ms delay = 100ms minimum elapsed.
        Assert.True(sw.ElapsedMilliseconds >= 90,
            $"Expected at least 90ms elapsed, got {sw.ElapsedMilliseconds}ms");
        Assert.Equal(3, callCount);
    }

    #endregion

    #region Support types

    private sealed record TestMessage(Guid Id)
    {
        public TestMessage() : this(Guid.NewGuid())
        {
            
        }
    };

    private sealed class NoOpConsumer : IConsumer<TestMessage>
    {
        public Task Consume(TestMessage message, IConsumeContext context,
            CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }

    #endregion
}
