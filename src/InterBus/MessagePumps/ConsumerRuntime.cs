using InterBus.Consuming;

namespace InterBus.MessagePumps;

/// <summary>Runs one consumer's shared processing limit and ownership budget across all receive pumps.</summary>
public sealed class ConsumerRuntime(ConsumerDescriptor descriptor, DeliveryProcessor processor, Action<Exception>? reportFailure = null)
{
    /// <summary>Runs initialized sources until intake stops, then drains accepted deliveries using the processing token.</summary>
    public async Task RunAsync(IBufferedSource source, CancellationToken intakeToken, CancellationToken processingToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(source.PrefetchCount);
        var sink = new DeliverySink(checked(descriptor.ConcurrencyCount + source.PrefetchCount));
        using var intake = CancellationTokenSource.CreateLinkedTokenSource(intakeToken);
        using var concurrency = new SemaphoreSlim(descriptor.ConcurrencyCount);
        var producers = Enumerable.Range(0, descriptor.PumpCount).Select(_ => ProduceAsync()).ToArray();
        var drain = DrainAsync();
        try
        {
            var receives = Task.WhenAll(producers);
            if (await Task.WhenAny(receives, source.Failure) == source.Failure)
            {
                var failure = await source.Failure;
                reportFailure?.Invoke(failure);
                await intake.CancelAsync();
                await receives;
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
            }
            await receives;
        }
        finally { sink.Complete(); await drain; }

        async Task ProduceAsync()
        {
            try
            {
                await source.ReceiveAsync(sink, intake.Token);
                if (!intake.IsCancellationRequested)
                    throw new InvalidOperationException("Buffered source stopped unexpectedly");
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

    /// <summary>Runs processing inside transport-managed delivery lifetimes without an additional buffer.</summary>
    public async Task RunAsync(IManagedSource source, CancellationToken intakeToken, CancellationToken processingToken)
    {
        if (descriptor.PumpCount != 1)
            throw new InvalidOperationException("Transport-managed consumers support PumpCount = 1 only");
        var run = source.RunAsync((delivery, token) => processor.ProcessAsync(descriptor, delivery, token),
            intakeToken, processingToken);
        Exception? failure = null;
        if (await Task.WhenAny(run, source.Failure) == source.Failure)
        {
            failure = await source.Failure;
            reportFailure?.Invoke(failure);
        }
        await run;
        if (failure is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
    }
}
