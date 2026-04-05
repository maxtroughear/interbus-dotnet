namespace InterBus.Internal;

/// <inheritdoc/>
internal sealed class ConsumerBuilder(ConsumerDescriptor descriptor) : IConsumerBuilder
{
    /// <inheritdoc/>
    public IConsumerBuilder WithConcurrency(int concurrencyCount)
    {
        if (concurrencyCount < 1)
            throw new ArgumentOutOfRangeException(
                nameof(concurrencyCount), "Concurrency count must be at least 1.");

        descriptor.ConcurrencyCount = concurrencyCount;
        return this;
    }
}
