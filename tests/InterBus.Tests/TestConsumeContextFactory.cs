using InterBus.Consumer;

namespace InterBus.Tests;

/// <summary>
/// Helper for creating <see cref="ConsumeContext"/> instances in tests.
/// Uses the internal constructor exposed via InternalsVisibleTo.
/// </summary>
internal static class TestConsumeContextFactory
{
    public static ConsumeContext<T> Create<T>(ConsumerDescriptor descriptor) where T : new() =>
        new(descriptor, new T(), new MessageHeaders());

    public static ConsumeContext<T> Create<T>(ConsumerDescriptor descriptor, MessageHeaders headers) where T : new() =>
        new(descriptor, new T(), headers);
}
