namespace InterBus;

/// <summary>
/// Provides access to the headers carried by the message currently being consumed.
/// Resolve via <see cref="IMessageContextAccessor"/> from DI inside a consumer or its dependencies.
/// </summary>
public interface IMessageContext
{
    /// <summary>The headers attached to the current message.</summary>
    IReadOnlyDictionary<string, string> Headers { get; }
}
