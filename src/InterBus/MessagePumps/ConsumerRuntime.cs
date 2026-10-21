using InterBus.Consuming;

namespace InterBus.MessagePumps;

/// <summary>Runs one consumer's shared processing limit and ownership budget across all receive pumps.</summary>
public sealed class ConsumerRuntime(ConsumerDescriptor descriptor, DeliveryProcessor processor, Action<Exception>? reportFailure = null)
{
    internal Action<PollResult?, Exception?>? ObserveAcquisition { get; init; }

    /// <summary>Runs initialized sources until intake stops, then drains accepted deliveries using the processing token.</summary>
    public async Task RunAsync(IBufferedPollingSource source, CancellationToken intakeToken, CancellationToken processingToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(source.PrefetchCount);
        ArgumentOutOfRangeException.ThrowIfLessThan(source.MaxBatchSize, 1);
        var sink = new DeliverySink(checked(descriptor.ConcurrencyCount + source.PrefetchCount));
        using var intake = CancellationTokenSource.CreateLinkedTokenSource(intakeToken);
        using var concurrency = new SemaphoreSlim(descriptor.ConcurrencyCount);
        var producers = Enumerable.Range(0, descriptor.PumpCount).Select(_ => ProduceAsync()).ToArray();
        var drain = DrainAsync();
        try
        {
            await ObserveSourceAsync(Task.WhenAll(producers), source, intake);
        }
        finally { sink.Complete(); await drain; }

        async Task ProduceAsync()
        {
            try
            {
                await PollAsync(source, async () =>
                {
                    var reservations = new List<DeliverySink.Reservation> { await sink.ReserveAsync(intake.Token) };
                    using var batch = new ReceiveBatch(reservations);
                    while (reservations.Count < source.MaxBatchSize && sink.TryReserve() is { } extra)
                        reservations.Add(extra);
                    return await source.ReceiveOnceAsync(batch, intake.Token);
                }, intake.Token);
            }
            catch (OperationCanceledException) when (intake.IsCancellationRequested) { }
            catch (Exception error) { reportFailure?.Invoke(error); await intake.CancelAsync(); throw; }
        }

        async Task DrainAsync()
        {
            var active = new List<Task>();
            var tails = new Dictionary<string, Task>(StringComparer.Ordinal);
            await foreach (var entry in sink.Reader.ReadAllAsync())
            {
                active.RemoveAll(task => task.IsCompletedSuccessfully);
                var key = entry.Delivery.OrderingKey;
                var previous = key is not null && tails.TryGetValue(key, out var tail) ? tail : Task.CompletedTask;
                var task = ProcessAsync(entry, previous);
                active.Add(task);
                if (key is not null) tails[key] = task;
                foreach (var completed in tails.Where(pair => pair.Value.IsCompletedSuccessfully).Select(pair => pair.Key).ToArray())
                    tails.Remove(completed);
            }
            await Task.WhenAll(active);
        }

        async Task ProcessAsync(DeliverySink.Entry entry, Task previous)
        {
            var entered = false;
            var transferred = false;
            try
            {
                await previous;
                await concurrency.WaitAsync(processingToken);
                entered = true;
                transferred = true;
                await processor.ProcessAsync(descriptor, entry.Delivery, processingToken);
            }
            catch (OperationCanceledException) when (processingToken.IsCancellationRequested) { }
            catch (Exception error) { reportFailure?.Invoke(error); await intake.CancelAsync(); throw; }
            finally
            {
                try { if (!transferred) await entry.Delivery.DisposeAsync(); }
                finally { if (entered) concurrency.Release(); entry.Capacity.Release(); }
            }
        }
    }

    /// <summary>Runs a fixed set of core workers over an existing transport input, without adding another queue.</summary>
    /// <remarks>A worker stops reading when intake is canceled and awaits its accepted delivery before returning.
    /// A worker fault stops sibling intake; every worker is awaited before this operation completes.</remarks>
    public static async Task RunWorkersAsync(int workerCount, Func<CancellationToken, Task> worker,
        CancellationToken intakeToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(workerCount, 1);
        using var intake = CancellationTokenSource.CreateLinkedTokenSource(intakeToken);
        await Task.WhenAll(Enumerable.Range(0, workerCount).Select(_ => RunAsync()));
        async Task RunAsync()
        {
            try { await worker(intake.Token); }
            catch (OperationCanceledException) when (intake.IsCancellationRequested) { }
            catch { await intake.CancelAsync(); throw; }
        }
    }

    /// <summary>Runs scoped iterations without buffering or holding resources behind ordering chains.</summary>
    public async Task RunAsync(IScopedPollingSource source, CancellationToken intakeToken, CancellationToken processingToken)
    {
        using var intake = CancellationTokenSource.CreateLinkedTokenSource(intakeToken);
        var run = RunWorkersAsync(descriptor.ConcurrencyCount, token => PollAsync(source, async () =>
        {
            var accepted = false;
            try
            {
                return await source.ProcessOnceAsync(async (delivery, ct) =>
                {
                    if (accepted) throw new InvalidOperationException("Scoped iteration accepted multiple deliveries");
                    accepted = true;
                    await processor.ProcessAsync(descriptor, delivery, ct);
                }, token, processingToken);
            }
            catch (OperationCanceledException) when (processingToken.IsCancellationRequested) { throw; }
            catch (Exception error) when (accepted) { throw new AcceptedIterationException(error); }
        }, token), intake.Token);
        await ObserveSourceAsync(run, source, intake);
    }

    /// <summary>Starts callback intake and retains source resources until pause has drained accepted work.</summary>
    public async Task RunAsync(ICallbackSource source, CancellationToken intakeToken, CancellationToken processingToken)
    {
        using var intake = CancellationTokenSource.CreateLinkedTokenSource(intakeToken);
        var run = RunCallbacksAsync();
        await ObserveSourceAsync(run, source, intake);
        async Task RunCallbacksAsync()
        {
            try
            {
                await source.StartAsync((delivery, token) => processor.ProcessAsync(descriptor, delivery, token), processingToken, intake.Token);
                await Task.Delay(Timeout.Infinite, intake.Token);
            }
            catch (OperationCanceledException) when (intake.IsCancellationRequested) { }
            finally
            {
                if (source is CallbackIntake control) await control.StopAsync();
                else await source.PauseAsync(CancellationToken.None);
            }
        }
    }

    private sealed class AcceptedIterationException(Exception error) : Exception("Scoped iteration failed after delivery acceptance", error);

    private async Task PollAsync(IConsumerSource source, Func<Task<PollResult>> iteration, CancellationToken token)
    {
        var attempt = 0;
        try
        {
            while (!token.IsCancellationRequested)
            {
                PollResult result;
                try
                {
                    result = await iteration();
                    attempt = 0;
                    ObserveAcquisition?.Invoke(result, null);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { return; }
                catch (Exception error)
                {
                    if (error is not AcceptedIterationException) ObserveAcquisition?.Invoke(null, error);
                    var delay = error is AcceptedIterationException ? null : (source as IPollingRetryPolicy)?.GetRetryDelay(error, ++attempt);
                    if (delay is null) throw;
                    ArgumentOutOfRangeException.ThrowIfLessThan(delay.Value, TimeSpan.Zero);
                    await Task.Delay(delay.Value, token);
                    continue;
                }
                if (!result.FoundWork && result.IdleWait is { } wait) await wait(token);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (Exception error) { reportFailure?.Invoke(error); throw; }
    }

    private async Task ObserveSourceAsync(Task run, IConsumerSource source, CancellationTokenSource? intake = null)
    {
        var health = source.Failure;
        Exception? failure = null;
        // Intake completion can win WhenAny even when a source failure has already been published
        await Task.WhenAny(run, health);
        await ObserveFailureAsync();
        try { await run; }
        catch (OperationCanceledException) when (health.IsCompletedSuccessfully)
        {
            await ObserveFailureAsync();
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure!).Throw();
            throw;
        }
        await ObserveFailureAsync();
        if (failure is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();

        async Task ObserveFailureAsync()
        {
            if (failure is not null || !health.IsCompleted) return;
            failure = await health;
            reportFailure?.Invoke(failure);
            if (intake is not null) await intake.CancelAsync();
        }
    }
}
