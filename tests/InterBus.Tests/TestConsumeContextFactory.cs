using InterBus.Middleware;

namespace InterBus.Tests;

/// <summary>
/// Helper for creating <see cref="ConsumeContext"/> instances in tests.
/// Uses the internal constructor exposed via InternalsVisibleTo.
/// </summary>
internal static class TestConsumeContextFactory
{
    public static ConsumeContext Create(ConsumerDescriptor descriptor) =>
        new(descriptor, "{}", new MessageHeaders());
}
