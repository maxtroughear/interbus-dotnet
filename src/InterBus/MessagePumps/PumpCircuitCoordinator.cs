using InterBus.Dispatching;
using InterBus.Resilience;

namespace InterBus.MessagePumps;

internal sealed class PumpCircuitCoordinator(PumpCircuitBreakerOptions options, TimeProvider clock,
    Action<PumpCircuitSnapshot>? transition = null, Action<Exception>? diagnosticFailure = null)
{
    private sealed class Policy(string name, CircuitBreakerOptions settings)
    {
        public readonly string Name = name;
        public readonly CircuitBreakerOptions Settings = settings;
        public readonly Queue<(DateTimeOffset Time, bool Failure)> Samples = new();
        public string State = "Closed";
        public DateTimeOffset Deadline;
        public int Successes;
        public long Transitions;
        public string? Reason;
    }

    private readonly TimeProvider _clock = clock;
    private readonly PumpCircuitBreakerOptions _options = options;
    private readonly object _gate = new();
    private readonly Policy? _dependency = options.DependencyPolicy is { } dependency ? new("Dependency", dependency) : null;
    private readonly Policy? _transport = options.TransportPolicy is { } transport ? new("Transport", transport) : null;
    private TaskCompletionSource _changed = Signal();
    private long _generation;
    private readonly Dictionary<long, int> _inflight = [];
    private int _probes;
    private bool _terminal;
    private readonly HashSet<ITimer> _timers = [];
    private DateTimeOffset _emptyUntil;
    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private IEnumerable<Policy> Policies => new[] { _dependency, _transport }.OfType<Policy>();
    internal Action<PumpObservation>? Observe { get; init; }
    internal void Publish(Lease lease, Guid? delivery, PumpStage stage, PumpOutcome outcome,
        Exception? error = null, DispatchResult? result = null, PumpCancellation cancellation = PumpCancellation.None,
        bool? recoverable = null, TimeSpan? retryDelay = null)
    {
        try { Observe?.Invoke(new(lease.OperationId, delivery, lease.Generation, lease.Probe, stage, outcome,
            _clock.GetUtcNow(), error, result, cancellation, recoverable, retryDelay)); }
        catch (Exception diagnosticError) { ReportDiagnostic(diagnosticError); }
    }
    private void ReportDiagnostic(Exception error)
    {
        try { diagnosticFailure?.Invoke(error); }
        catch { /* Diagnostic failures must not escape into transport handling */ }
    }
    internal bool Closed { get { lock (_gate) return Policies.All(p => p.State == "Closed"); } }
    internal IReadOnlyList<PumpCircuitSnapshot> Snapshots
    {
        get { lock (_gate) return Policies.Select(Snapshot).ToArray(); }
    }
    private PumpCircuitSnapshot Snapshot(Policy policy) => new(policy.Name, policy.State, _generation,
        policy.State == "Closed" ? null : policy.Deadline, _probes, policy.Transitions, policy.Reason);
    private void Wake() { var previous = _changed; _changed = Signal(); previous.TrySetResult(); }
    private void Change(Policy policy, string state, string reason)
    {
        policy.State = state;
        policy.Reason = reason;
        policy.Transitions++;
        try { transition?.Invoke(Snapshot(policy)); }
        catch (Exception diagnosticError) { ReportDiagnostic(diagnosticError); }
        Wake();
    }
    internal Task WaitForChangeAsync(CancellationToken token)
    { lock (_gate) return _changed.Task.WaitAsync(token); }
    internal Lease AcceptCallback()
    {
        lock (_gate)
        {
            var generation = Closed ? _generation : -1;
            _inflight[generation] = _inflight.GetValueOrDefault(generation) + 1;
            return new Lease(this, generation, false, 1, []);
        }
    }
    internal void Stop()
    {
        lock (_gate)
        {
            _terminal = true;
            foreach (var timer in _timers) timer.Dispose();
            _timers.Clear();
            Wake();
        }
    }

    internal async Task WaitAsync(CancellationToken token)
    {
        while (true)
        {
            Task changed;
            TimeSpan delay;
            lock (_gate)
            {
                token.ThrowIfCancellationRequested();
                if (_terminal) throw new OperationCanceledException(token);
                var now = _clock.GetUtcNow();
                if (Policies.All(p => p.State == "Closed")) return;
                var deadline = Policies.Where(p => p.State != "Closed").Max(p => p.Deadline);
                if (_emptyUntil > deadline) deadline = _emptyUntil;
                if (now >= deadline && (!_inflight.Any(pair => pair.Key != _generation)) && _probes < _options.ProbeDeliveryLimit) return;
                changed = _changed.Task;
                delay = deadline > now ? deadline - now : Timeout.InfiniteTimeSpan;
            }
            using var wait = CancellationTokenSource.CreateLinkedTokenSource(token);
            var timer = Task.Delay(delay, _clock, wait.Token);
            try { await (await Task.WhenAny(changed, timer)).WaitAsync(token); }
            finally { await wait.CancelAsync(); }
        }
    }

    internal Lease? TryBegin(int maximum)
    {
        lock (_gate)
        {
            if (_terminal) return null;
            var open = Policies.Where(p => p.State != "Closed").ToArray();
            var probe = open.Length != 0;
            if (probe)
            {
                var now = _clock.GetUtcNow();
                if (_inflight.Any(pair => pair.Key != _generation) || now < _emptyUntil || open.Any(p => now < p.Deadline) || _probes >= _options.ProbeDeliveryLimit)
                    return null;
                foreach (var policy in open.Where(p => p.State == "Open")) Change(policy, "HalfOpen", "Break elapsed and accepted work drained");
                maximum = 1;
            }
            _inflight[_generation] = _inflight.GetValueOrDefault(_generation) + 1;
            if (probe) _probes += maximum;
            return new Lease(this, _generation, probe, maximum, probe ? open.Select(p => p.Name).ToArray() : []);
        }
    }

    private void Open(Policy policy, string reason)
    {
        _generation++;
        foreach (var current in Policies) current.Successes = 0;
        policy.Samples.Clear();
        policy.Deadline = _clock.GetUtcNow() + policy.Settings.BreakDuration +
            TimeSpan.FromTicks((long)(Random.Shared.NextDouble() * _options.ReopenJitter.Ticks));
        Change(policy, "Open", reason);
    }
    private void Sample(Policy? policy, Lease lease, bool failure, string reason)
    {
        if (policy is null || lease.Generation != _generation || _terminal) return;
        if (policy.State != "Closed")
        {
            if (failure && lease.Probe) Open(policy, reason);
            return;
        }
        var now = _clock.GetUtcNow();
        while (policy.Samples.TryPeek(out var sample) && now - sample.Time >= policy.Settings.SamplingDuration) policy.Samples.Dequeue();
        policy.Samples.Enqueue((now, failure));
        if (failure && policy.Samples.Count >= policy.Settings.MinimumThroughput &&
            (double)policy.Samples.Count(s => s.Failure) / policy.Samples.Count >= policy.Settings.FailureThreshold)
            Open(policy, reason);
    }
    internal void Dispatch(Lease lease, DispatchResult result, bool decodeError, bool canceled)
    {
        if (decodeError || canceled) return;
        bool failure;
        try { failure = _options.DependencyFailure(result); }
        catch (Exception error) { ReportDiagnostic(error); return; }
        lock (_gate) Sample(_dependency, lease, failure, result.Exception?.Message ?? "Dispatch failure");
    }
    internal void Settlement(Lease lease, bool success)
    {
        if (!_options.ObserveSettlementFailures) return;
        lock (_gate) Sample(_transport, lease, !success, "Settlement failed or rejected");
    }
    internal void Acquisition(Lease lease, bool foundWork, bool? recoverableFailure, Exception? error = null, TimeSpan? retryDelay = null)
    {
        Publish(lease, null, PumpStage.Acquisition, error is not null ? PumpOutcome.Failure : foundWork ? PumpOutcome.Success : PumpOutcome.Empty,
            error, recoverable: recoverableFailure, retryDelay: retryDelay);
        lock (_gate)
        {
            if (recoverableFailure is { } failure && (error is null || failure)) Sample(_transport, lease, failure, "Recoverable acquisition failure");
            if (!foundWork && lease.Probe) _emptyUntil = _clock.GetUtcNow() + _options.EmptyProbeDelay;
            if (lease.Probe && error is null && recoverableFailure == false && !_options.ObserveSettlementFailures) Recover(_transport, lease);
        }
    }
    private void Recover(Policy? policy, Lease lease)
    {
        if (policy is null || !lease.Policies.Contains(policy.Name) || lease.Generation != _generation || policy.State != "HalfOpen" || _terminal) return;
        if (++policy.Successes >= _options.SuccessfulProbesRequired)
        {
            policy.Samples.Clear();
            policy.Successes = 0;
            Change(policy, "Closed", "Recovery evidence completed");
        }
    }
    private void Complete(Lease lease)
    {
        lock (_gate)
        {
            if (lease.Probe && lease.Generation == _generation && !_terminal)
            {
                if (lease.SuccessfulDelivery)
                {
                    Recover(_dependency, lease);
                    if (_options.ObserveSettlementFailures) Recover(_transport, lease);
                }
                else if (lease.HadDelivery || lease.TimedOut)
                    foreach (var policy in Policies.Where(p => lease.Policies.Contains(p.Name) && p.State != "Closed").ToArray())
                        Open(policy, lease.TimedOut ? "Probe deadline expired" : "Probe completion uncertain or unsuccessful");
            }
            if (--_inflight[lease.Generation] == 0) _inflight.Remove(lease.Generation);
            if (lease.Probe) _probes -= lease.Capacity;
            if (lease.Probe || Policies.Any(p => p.State != "Closed")) Wake();
        }
    }

    internal sealed class Lease : IDisposable
    {
        public Guid OperationId { get; } = Guid.NewGuid();
        private readonly PumpCircuitCoordinator _owner;
        private readonly ITimer? _timer;
        private int _references = 1;
        private int _successes;
        private int _deliveries;
        private int _failed;
        public long Generation { get; }
        public bool Probe { get; }
        public int Capacity { get; }
        public string[] Policies { get; }
        private int _timedOut;
        public bool TimedOut => Volatile.Read(ref _timedOut) != 0;
        public bool HadDelivery => _deliveries != 0;
        public bool SuccessfulDelivery => !TimedOut && _failed == 0 && _successes > 0;
        internal Lease(PumpCircuitCoordinator owner, long generation, bool probe, int capacity, string[] policies)
        {
            _owner = owner; Generation = generation; Probe = probe; Capacity = capacity; Policies = policies;
            if (probe)
            {
                _timer = owner._clock.CreateTimer(_ => Timeout(), null, owner._options.ProbeTimeout, System.Threading.Timeout.InfiniteTimeSpan);
                owner._timers.Add(_timer);
            }
        }
        internal void Expire() => Timeout();
        private void Timeout()
        {
            lock (_owner._gate)
            {
                if (_references == 0) return;
                if (Interlocked.Exchange(ref _timedOut, 1) != 0) return;
                _owner.Publish(this, null, PumpStage.ProbeDeadline, PumpOutcome.Indeterminate,
                    cancellation: PumpCancellation.AcquisitionDeadline);
                if (Generation != _owner._generation || _owner._terminal) return;
                foreach (var policy in _owner.Policies.Where(p => Policies.Contains(p.Name) && p.State != "Closed").ToArray())
                    _owner.Open(policy, "Probe deadline expired; retaining accepted resources");
            }
        }
        public CircuitDelivery Wrap(IDelivery delivery)
        {
            Interlocked.Increment(ref _references);
            Interlocked.Increment(ref _deliveries);
            return new CircuitDelivery(delivery, this, _owner);
        }
        public void DeliveryCompleted(bool success)
        {
            if (success) Interlocked.Increment(ref _successes);
            else Interlocked.Exchange(ref _failed, 1);
            Dispose();
        }
        public void ScopeCompleted(Exception? error = null)
        {
            if (error is not null) Fail();
            _owner.Publish(this, null, PumpStage.Scope, error is null ? PumpOutcome.Success : PumpOutcome.Failure, error);
        }
        public void Retain() => Interlocked.Increment(ref _references);
        public void Fail() => Interlocked.Exchange(ref _failed, 1);
        public void Dispose()
        {
            if (Interlocked.Decrement(ref _references) != 0) return;
            if (_timer is not null)
            {
                lock (_owner._gate) { _timer.Dispose(); _owner._timers.Remove(_timer); }
            }
            _owner.Publish(this, null, PumpStage.Completion, SuccessfulDelivery ? PumpOutcome.Success : HadDelivery ? PumpOutcome.Indeterminate : PumpOutcome.Empty);
            _owner.Complete(this);
        }
    }
}

internal sealed class CircuitDelivery(IDelivery delivery, PumpCircuitCoordinator.Lease lease, PumpCircuitCoordinator coordinator) : IDelivery
{
    private readonly Guid _id = Guid.NewGuid();
    private bool _dispatched;
    private bool _settled;
    private bool _canceled;
    public ReadOnlyMemory<byte> Payload => delivery.Payload;
    public MessageHeaders Headers => delivery.Headers;
    public Exception? DecodeError => delivery.DecodeError;
    public string? OrderingKey => delivery.OrderingKey;
    public void ObserveDispatch(DispatchResult result, bool canceled)
    {
        _dispatched = !canceled && DecodeError is null && result.Kind == DispatchResultKind.Success;
        _canceled = canceled;
        coordinator.Publish(lease, _id, PumpStage.Decode, DecodeError is null ? PumpOutcome.Success : PumpOutcome.Failure, DecodeError);
        coordinator.Publish(lease, _id, PumpStage.Dispatch, canceled ? PumpOutcome.Canceled : _dispatched ? PumpOutcome.Success : PumpOutcome.Failure,
            result.Exception, result, canceled ? PumpCancellation.Shutdown : PumpCancellation.None);
        coordinator.Dispatch(lease, result, DecodeError is not null, canceled);
    }
    public async Task<bool> SettleAsync(DispatchResult result, CancellationToken token)
    {
        try
        {
            _settled = await delivery.SettleAsync(result, token);
            _canceled |= token.IsCancellationRequested;
            coordinator.Publish(lease, _id, PumpStage.Settlement, _settled ? PumpOutcome.Success : PumpOutcome.Rejected, result: result);
            if (!token.IsCancellationRequested) coordinator.Settlement(lease, _settled);
            return _settled;
        }
        catch (Exception error)
        {
            coordinator.Publish(lease, _id, PumpStage.Settlement, token.IsCancellationRequested ? PumpOutcome.Canceled : PumpOutcome.Indeterminate,
                error, result, token.IsCancellationRequested ? PumpCancellation.Shutdown : PumpCancellation.None);
            if (!token.IsCancellationRequested) coordinator.Settlement(lease, false);
            throw;
        }
    }
    public async ValueTask DisposeAsync()
    {
        var cleaned = false;
        try { await delivery.DisposeAsync(); cleaned = true; }
        finally
        {
            coordinator.Publish(lease, _id, PumpStage.Cleanup, cleaned ? PumpOutcome.Success : PumpOutcome.Failure);
            lease.DeliveryCompleted(_dispatched && _settled && cleaned && !_canceled);
        }
    }
}
