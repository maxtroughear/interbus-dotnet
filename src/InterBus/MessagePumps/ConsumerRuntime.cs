using InterBus.Consuming;

namespace InterBus.MessagePumps;

/// <summary>Runs one consumer's shared processing limit and ownership budget across all receive pumps.</summary>
public sealed class ConsumerRuntime(ConsumerDescriptor descriptor, DeliveryProcessor processor, Action<Exception>? reportFailure = null, TimeProvider? timeProvider = null)
{
    private readonly TimeProvider _clock = timeProvider ?? TimeProvider.System;
    private readonly PumpCircuitCoordinator? _circuit = descriptor.PumpCircuitBreaker is { } options
        ? new(options, timeProvider ?? TimeProvider.System, snapshot => processor.LogCircuit(descriptor.ConsumerType, snapshot), processor.LogCircuitDiagnostic) : null;
    internal IReadOnlyList<InterBus.Resilience.PumpCircuitSnapshot> CircuitSnapshots => _circuit?.Snapshots ?? [];
    internal Action<PollResult?, Exception?>? ObserveAcquisition { get; init; }

    /// <summary>Runs initialized sources until intake stops, then drains accepted deliveries using the processing token.</summary>
    public async Task RunAsync(IBufferedPollingSource source, CancellationToken intakeToken, CancellationToken processingToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(source.PrefetchCount);
        ArgumentOutOfRangeException.ThrowIfLessThan(source.MaxBatchSize, 1);
        var sink = new DeliverySink(checked(descriptor.ConcurrencyCount + source.PrefetchCount));
        using var intake = CancellationTokenSource.CreateLinkedTokenSource(intakeToken);
        using var terminal = intake.Token.Register(() => _circuit?.Stop());
        using var concurrency = new SemaphoreSlim(descriptor.ConcurrencyCount);
        var producers = Enumerable.Range(0, descriptor.PumpCount).Select(_ => ProduceAsync()).ToArray();
        var drain = DrainAsync();
        try
        {
            await ObserveSourceAsync(Task.WhenAll(producers), source, intake);
        }
        finally { _circuit?.Stop(); sink.Complete(); await drain; }

        async Task ProduceAsync()
        {
            try
            {
                await PollAsync(source, async () =>
                {
                    var reservations = new List<DeliverySink.Reservation> { await sink.ReserveAsync(intake.Token) };
                    using var batch = new ReceiveBatch(reservations);
                    using var admission = _circuit?.TryBegin(source.MaxBatchSize);
                    if (_circuit is not null && admission is null) return new PollResult(false);
                    var maximum = admission?.Capacity ?? source.MaxBatchSize;
                    batch.Wrap = admission is null ? null : d => admission.Wrap(d);
                    while (reservations.Count < maximum && sink.TryReserve() is { } extra)
                        reservations.Add(extra);
                    using var deadline = admission?.Probe == true ? new CancellationTokenSource(descriptor.PumpCircuitBreaker!.ProbeTimeout, _clock) : null;
                    using var operation = deadline is null ? null : CancellationTokenSource.CreateLinkedTokenSource(intake.Token, deadline.Token);
                    try
                    {
                        var result = await source.ReceiveOnceAsync(batch, operation?.Token ?? intake.Token);
                        if (admission is not null) _circuit!.Acquisition(admission, result.FoundWork, false);
                        return result;
                    }
                    catch (OperationCanceledException) when (deadline?.IsCancellationRequested == true && !intake.IsCancellationRequested)
                    { admission!.Expire(); return new PollResult(false); }
                    catch (Exception error) when (error is not OperationCanceledException || !intake.IsCancellationRequested)
                    { admission?.Retain(); throw new AcquisitionException(error, admission); }
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
        using var terminal = intake.Token.Register(() => _circuit?.Stop());
        var run = RunWorkersAsync(descriptor.ConcurrencyCount, token => PollAsync(source, async () =>
        {
            using var admission = _circuit?.TryBegin(1);
            if (_circuit is not null && admission is null) return new PollResult(false);
            var accepted = false;
            using var deadline = admission?.Probe == true ? new CancellationTokenSource(descriptor.PumpCircuitBreaker!.ProbeTimeout, _clock) : null;
            using var operation = deadline is null ? null : CancellationTokenSource.CreateLinkedTokenSource(token, deadline.Token);
            try
            {
                var result = await source.ProcessOnceAsync(async (delivery, ct) =>
                {
                    if (accepted) throw new InvalidOperationException("Scoped iteration accepted multiple deliveries");
                    accepted = true;
                    await processor.ProcessAsync(descriptor, admission?.Wrap(delivery) ?? delivery, ct);
                }, operation?.Token ?? token, processingToken);
                admission?.ScopeCompleted();
                if (admission is not null) _circuit!.Acquisition(admission, result.FoundWork, false);
                return result;
            }
            catch (OperationCanceledException) when (!accepted && deadline?.IsCancellationRequested == true && !token.IsCancellationRequested)
            { admission!.Expire(); return new PollResult(false); }
            catch (OperationCanceledException) when (processingToken.IsCancellationRequested) { throw; }
            catch (Exception error) when (accepted) { admission?.ScopeCompleted(error); throw new AcceptedIterationException(error); }
            catch (Exception error)
            {
                admission?.Retain();
                throw new AcquisitionException(error, admission);
            }
        }, token), intake.Token);
        try { await ObserveSourceAsync(run, source, intake); }
        finally { _circuit?.Stop(); }
    }

    /// <summary>Starts callback intake and retains source resources until pause has drained accepted work.</summary>
    public async Task RunAsync(ICallbackSource source, CancellationToken intakeToken, CancellationToken processingToken)
    {
        using var intake = CancellationTokenSource.CreateLinkedTokenSource(intakeToken);
        using var terminal = intake.Token.Register(() => _circuit?.Stop());
        if (source is CallbackIntake controls) controls.AllowResume = () => _circuit?.Closed ?? true;
        PumpCircuitCoordinator.Lease? grant = null;
        var healthSource = source as IAcquisitionHealthSource;
        if (_circuit is not null && healthSource is not null) healthSource.AcquisitionFailed += AcquisitionFailed;
        var run = RunCallbacksAsync();
        await ObserveSourceAsync(run, source, intake);
        void AcquisitionFailed(Exception error)
        {
            using var observation = _circuit!.AcceptCallback();
            _circuit.Acquisition(observation, false, true, error);
        }
        async Task RunCallbacksAsync()
        {
            try
            {
                await source.StartAsync(async (delivery, token) =>
                {
                    using var accepted = grant is null ? _circuit?.AcceptCallback() : null;
                    var lease = grant ?? accepted;
                    if (accepted is not null) _circuit!.Acquisition(accepted, true, false);
                    await processor.ProcessAsync(descriptor, lease?.Wrap(delivery) ?? delivery, token);
                }, processingToken, intake.Token);
                if (_circuit is null) await Task.Delay(Timeout.Infinite, intake.Token);
                else
                {
                    while (!intake.IsCancellationRequested)
                    {
                        var changed = _circuit.WaitForChangeAsync(intake.Token);
                        if (_circuit.Closed) { await changed; continue; }
                        await source.PauseAsync(intake.Token);
                        while (!_circuit.Closed)
                        {
                            await _circuit.WaitAsync(intake.Token);
                            using var admission = _circuit.TryBegin(1);
                            if (admission is null) continue;
                            grant = admission;
                            using var deadline = new CancellationTokenSource(descriptor.PumpCircuitBreaker!.ProbeTimeout, _clock);
                            using var operation = CancellationTokenSource.CreateLinkedTokenSource(intake.Token, deadline.Token);
                            try
                            {
                                var result = await ((IProbeControlledCallbackSource)source).ReceiveProbeAsync(processingToken, operation.Token);
                                admission.ScopeCompleted();
                                _circuit.Acquisition(admission, result.FoundWork, false);
                            }
                            catch (OperationCanceledException) when (deadline.IsCancellationRequested && !intake.IsCancellationRequested)
                            { admission.Fail(); admission.Expire(); }
                            catch (Exception error) when ((source as IPollingFailureClassifier)?.IsRecoverable(error) == true)
                            {
                                admission.ScopeCompleted(error);
                                if (!admission.HadDelivery) _circuit.Acquisition(admission, false, true, error);
                            }
                            finally { grant = null; }
                        }
                        await source.ResumeAsync(intake.Token);
                    }
                }
            }
            catch (OperationCanceledException) when (intake.IsCancellationRequested) { }
            finally
            {
                _circuit?.Stop();
                if (healthSource is not null) healthSource.AcquisitionFailed -= AcquisitionFailed;
                if (source is CallbackIntake control) await control.StopAsync();
                else await source.PauseAsync(CancellationToken.None);
            }
        }
    }

    private void ObserveSafely(PollResult? result, Exception? error)
    {
        try { ObserveAcquisition?.Invoke(result, error); }
        catch { /* Diagnostic observers cannot affect acquisition or settlement */ }
    }

    private sealed class AcquisitionException(Exception error, PumpCircuitCoordinator.Lease? lease) : Exception("Acquisition failed", error)
    { public PumpCircuitCoordinator.Lease? Lease { get; } = lease; }

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
                    if (_circuit is not null) await _circuit.WaitAsync(token);
                    result = await iteration();
                    attempt = 0;
                    ObserveSafely(result, null);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { return; }
                catch (Exception error)
                {
                    var acquisition = error as AcquisitionException;
                    var original = acquisition?.InnerException ?? error;
                    TimeSpan? delay;
                    try
                    {
                        delay = error is AcceptedIterationException ? null : (source as IPollingRetryPolicy)?.GetRetryDelay(original, ++attempt);
                        if (error is not AcceptedIterationException) ObserveSafely(null, original);
                        if (acquisition?.Lease is { } lease)
                            _circuit!.Acquisition(lease, false, (source as IPollingFailureClassifier)?.IsRecoverable(original) ?? delay is not null, original, delay);
                    }
                    finally { acquisition?.Lease?.Dispose(); }
                    if (delay is null)
                    {
                        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(original).Throw();
                        throw;
                    }
                    ArgumentOutOfRangeException.ThrowIfLessThan(delay.Value, TimeSpan.Zero);
                    await Task.Delay(delay.Value, _clock, token);
                    continue;
                }
                if (result.IdleWait is { } wait)
                {
                    if (_circuit is null) await wait(token);
                    else
                    {
                        using var idle = CancellationTokenSource.CreateLinkedTokenSource(token);
                        var changed = _circuit.WaitForChangeAsync(idle.Token);
                        var waiting = _circuit.Closed ? wait(idle.Token).AsTask() : Task.CompletedTask;
                        try { await await Task.WhenAny(waiting, changed); }
                        finally
                        {
                            await idle.CancelAsync();
                            try { await waiting; }
                            catch (OperationCanceledException) when (idle.IsCancellationRequested) { }
                            try { await changed; }
                            catch (OperationCanceledException) when (idle.IsCancellationRequested) { }
                        }
                    }
                }
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
