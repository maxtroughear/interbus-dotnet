namespace InterBus;

/// <summary>
/// A case-insensitive string dictionary that carries arbitrary key/value metadata alongside a message.
/// Users may populate headers before publishing; InterBus may also add its own internal headers.
/// </summary>
public sealed class MessageHeaders : Dictionary<string, string>
{
    /// <summary>Initializes an empty <see cref="MessageHeaders"/> instance.</summary>
    public MessageHeaders() : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    /// <summary>Initializes a <see cref="MessageHeaders"/> instance copied from an existing dictionary.</summary>
    public MessageHeaders(IDictionary<string, string> headers)
        : base(headers, StringComparer.OrdinalIgnoreCase)
    {
    }
}
