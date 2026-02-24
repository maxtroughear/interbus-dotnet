namespace InterBus;

/// <summary>
/// Provides access to the <see cref="IMessageContext"/> for the message currently being dispatched.
/// Registered as a singleton by InterBus; the current context is stored in an <c>AsyncLocal</c>
/// so each concurrent dispatch has its own isolated value.
/// </summary>
public interface IMessageContextAccessor
{
    /// <summary>
    /// Gets or sets the <see cref="IMessageContext"/> for the current dispatch.
    /// Returns <c>null</c> when accessed outside of a message dispatch context.
    /// </summary>
    IMessageContext? MessageContext { get; set; }
}
