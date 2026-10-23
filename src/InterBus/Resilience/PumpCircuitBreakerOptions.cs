using InterBus.Dispatching;

namespace InterBus.Resilience;

/// <summary>Consumer-wide intake policies. Accepted deliveries always continue through cleanup.</summary>
public sealed class PumpCircuitBreakerOptions
{
    internal CircuitBreakerOptions? DependencyPolicy { get; private set; } = new();
    internal CircuitBreakerOptions? TransportPolicy { get; private set; }

    /// <summary>Enables and configures final dispatch failure sampling. Enabled by default.</summary>
    /// <param name="configure">Optional settings; omitted settings retain their existing values.</param>
    /// <returns>This configuration for chaining.</returns>
    public PumpCircuitBreakerOptions Dependency(Action<CircuitBreakerOptions>? configure = null)
    {
        DependencyPolicy ??= new();
        configure?.Invoke(DependencyPolicy);
        return this;
    }

    /// <summary>Enables and configures recoverable acquisition failure sampling. Disabled by default.</summary>
    /// <param name="configure">Optional settings; omitted settings retain their existing values.</param>
    /// <returns>This configuration for chaining.</returns>
    public PumpCircuitBreakerOptions Transport(Action<CircuitBreakerOptions>? configure = null)
    {
        TransportPolicy ??= new();
        configure?.Invoke(TransportPolicy);
        return this;
    }

    /// <summary>Disables final dispatch failure sampling without changing the transport policy.</summary>
    /// <returns>This configuration for chaining.</returns>
    public PumpCircuitBreakerOptions DisableDependency()
    {
        DependencyPolicy = null;
        return this;
    }

    /// <summary>Disables acquisition failure sampling without changing the dependency policy.</summary>
    /// <returns>This configuration for chaining.</returns>
    public PumpCircuitBreakerOptions DisableTransport()
    {
        TransportPolicy = null;
        return this;
    }

    /// <summary>Maximum concurrent recovery deliveries across the consumer.</summary>
    public int ProbeDeliveryLimit { get; set; } = 1;
    /// <summary>Successful probes required in one recovery round.</summary>
    public int SuccessfulProbesRequired { get; set; } = 1;
    /// <summary>Maximum time for a recovery operation, including processing. Expiry never cancels accepted work.</summary>
    public TimeSpan ProbeTimeout { get; set; } = TimeSpan.FromSeconds(30);
    /// <summary>Maximum random delay added to each break duration.</summary>
    public TimeSpan ReopenJitter { get; set; } = TimeSpan.FromSeconds(3);
    /// <summary>Delay between empty recovery acquisitions.</summary>
    public TimeSpan EmptyProbeDelay { get; set; } = TimeSpan.FromSeconds(1);
    /// <summary>Classifies final exception dispositions. Decode errors and expected cancellation are excluded first.</summary>
    public Func<DispatchResult, bool> DependencyFailure { get; set; } = result => result.Exception is not null;
    /// <summary>Includes failed or rejected settlement in the transport policy.</summary>
    public bool ObserveSettlementFailures { get; set; }

    internal void Validate()
    {
        static void CheckTimer(TimeSpan value, string name)
        {
            if (value.TotalMilliseconds > uint.MaxValue - 1) throw new ArgumentOutOfRangeException(name, "Duration exceeds the supported timer range");
        }
        CheckTimer(ProbeTimeout, nameof(ProbeTimeout));
        CheckTimer(EmptyProbeDelay, nameof(EmptyProbeDelay));
        CheckTimer(ReopenJitter, nameof(ReopenJitter));
        foreach (var policy in new[] { DependencyPolicy, TransportPolicy }.OfType<CircuitBreakerOptions>())
        {
            CheckTimer(policy.BreakDuration, nameof(CircuitBreakerOptions.BreakDuration));
            CheckTimer(policy.SamplingDuration, nameof(CircuitBreakerOptions.SamplingDuration));
        }
        DependencyPolicy?.Validate();
        TransportPolicy?.Validate();
        if (DependencyPolicy is null && TransportPolicy is null) throw new ArgumentException("Enable at least one pump circuit policy");
        ArgumentOutOfRangeException.ThrowIfLessThan(ProbeDeliveryLimit, 1);
        if (SuccessfulProbesRequired < 1 || SuccessfulProbesRequired > ProbeDeliveryLimit)
            throw new ArgumentOutOfRangeException(nameof(SuccessfulProbesRequired));
        if (ProbeTimeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(ProbeTimeout));
        if (ReopenJitter < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(ReopenJitter));
        if (EmptyProbeDelay <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(EmptyProbeDelay));
        ArgumentNullException.ThrowIfNull(DependencyFailure);
    }
}

/// <summary>Snapshot of one intake policy, independent of pump lifecycle state.</summary>
/// <param name="Policy">Dependency or transport.</param>
/// <param name="State">Closed, open or half-open.</param>
/// <param name="Generation">Generation used to exclude stale evidence.</param>
/// <param name="NextProbeAt">Earliest recovery opportunity.</param>
/// <param name="ActiveProbes">Outstanding recovery acquisitions and deliveries.</param>
/// <param name="Transitions">Number of policy state changes.</param>
/// <param name="Reason">Most recent transition reason.</param>
public sealed record PumpCircuitSnapshot(string Policy, string State, long Generation,
    DateTimeOffset? NextProbeAt, int ActiveProbes, long Transitions, string? Reason);
