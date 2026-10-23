using InterBus.Consuming;

namespace InterBus.Tests;

/// <summary>
/// Helper for creating <see cref="ConsumeContext"/> instances in tests.
/// </summary>
internal static class TestConsumeContextFactory
{
    internal static Type NoOpConsumer = typeof(NoOpConsumer);

    internal static ConsumerDescriptor NoOpDescriptor =
        new ConsumerDescriptor(NoOpConsumer, RoutingConventions.QueueUri("queue", "test-queue"));

    public static ConsumeContext<T> Create<T>(T message) => new(NoOpDescriptor, message, new MessageHeaders());

    public static ConsumeContext<T> Create<T>(T message, MessageHeaders headers) =>
        new(NoOpDescriptor, message, headers);

    public static ConsumeContext<T> Create<T>(ConsumerDescriptor descriptor) where T : new() =>
        new(descriptor, new T(), new MessageHeaders());

    public static ConsumeContext<T> Create<T>(ConsumerDescriptor descriptor, MessageHeaders headers) where T : new() =>
        new(descriptor, new T(), headers);
}

internal class NoOpConsumer;
