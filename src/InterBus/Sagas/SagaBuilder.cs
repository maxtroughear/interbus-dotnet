namespace InterBus.Sagas;

/// <inheritdoc/>
internal sealed class SagaBuilder<TSaga, TState> : ISagaBuilder
    where TSaga : class, ISaga<TState>
    where TState : class, new()
{
    /// <summary>The configured time-to-live for saga instances, if set.</summary>
    internal TimeSpan? TimeToLive { get; private set; }

    /// <summary>The configured concurrency count, if set.</summary>
    internal int? Concurrency { get; private set; }

    /// <inheritdoc/>
    public ISagaBuilder WithTimeToLive(TimeSpan ttl)
    {
        TimeToLive = ttl;
        return this;
    }

    /// <inheritdoc/>
    public ISagaBuilder WithConcurrency(int concurrencyCount)
    {
        if (concurrencyCount < 1)
            throw new ArgumentOutOfRangeException(
                nameof(concurrencyCount), "Concurrency count must be at least 1");

        Concurrency = concurrencyCount;
        return this;
    }
}
