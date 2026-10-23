using InterBus.Consuming;

namespace InterBus.MessagePumps;

/// <summary>Creates one buffered polling, scoped polling or callback transport adapter per consumer.</summary>
public interface IConsumerSourceFactory
{
    /// <summary>Creates uninitialized resources owned and asynchronously disposed by core supervision.</summary>
    IConsumerSource Create(ConsumerDescriptor descriptor);
}
