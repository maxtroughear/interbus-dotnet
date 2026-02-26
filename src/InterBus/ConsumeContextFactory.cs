namespace InterBus;

/// <summary>
/// Creates <see cref="IConsumeContext"/> instances from <see cref="MessageHeaders"/>.
/// Exposed as a public factory so source-generated dispatch delegates can create contexts
/// without accessing the internal <c>ConsumeContext</c> implementation directly.
/// </summary>
public static class ConsumeContextFactory
{
    /// <summary>Creates a new <see cref="IConsumeContext"/> for the given <paramref name="headers"/>.</summary>
    public static IConsumeContext Create(MessageHeaders headers) => new Internal.ConsumeContext(headers);
}
