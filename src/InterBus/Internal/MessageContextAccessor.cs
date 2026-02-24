namespace InterBus.Internal;

/// <summary>
/// Singleton implementation of <see cref="IMessageContextAccessor"/> backed by an <see cref="AsyncLocal{T}"/>.
/// Each independent async dispatch flow gets its own isolated value.
/// </summary>
internal sealed class MessageContextAccessor : IMessageContextAccessor
{
    private static readonly AsyncLocal<IMessageContext?> _current = new();

    public IMessageContext? MessageContext
    {
        get => _current.Value;
        set => _current.Value = value;
    }
}
