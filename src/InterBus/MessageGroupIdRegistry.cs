namespace InterBus;

/// <summary>
/// Singleton registry mapping message types to their message group ID selector delegates.
/// Populated via <c>builder.Configure&lt;T&gt;(c =&gt; c.Fifo(selector))</c> or
/// <c>builder.Configure&lt;T&gt;(c =&gt; c.GroupBy(selector))</c>.
/// <para>
/// The selector is evaluated by <see cref="Publishing.Publisher"/> when stamping outgoing
/// messages to produce the <see cref="InterBusHeaders.MessageGroupId"/> header value.
/// </para>
/// </summary>
public sealed class MessageGroupIdRegistry
{
    private readonly Dictionary<Type, Func<object, string>> _selectors =
        new();

    /// <summary>
    /// Registers a group ID selector for <typeparamref name="T"/>. Replaces any
    /// previously registered selector for the same type.
    /// </summary>
    public void Register<T>(Func<T, string> selector) where T : class =>
        _selectors[typeof(T)] = msg => selector((T)msg);

    /// <summary>
    /// Returns the group ID for <paramref name="message"/> using the registered selector,
    /// or <c>null</c> if no selector is registered for <typeparamref name="T"/>.
    /// </summary>
    public string? TryResolve<T>(T message) where T : class =>
        _selectors.TryGetValue(typeof(T), out var selector) ? selector(message) : null;
}
