using Microsoft.EntityFrameworkCore;

// ReSharper disable EntityFramework.ModelValidation.UnlimitedStringLength - set in ModeBuilder

namespace InterBus.EntityFrameworkCore.Sagas;

/// <summary>
/// EF Core entity representing a persisted saga instance.
/// One row per <c>(CorrelationId, SagaType)</c> pair; multiple saga types can share the same
/// <see cref="DbContext"/>.
/// </summary>
public sealed class SagaStateEntry
{
    /// <summary>
    /// Application-defined identifier that correlates messages to a specific saga instance.
    /// </summary>
    public required string CorrelationId { get; set; }

    /// <summary>
    /// Fully qualified name of the saga class, used to distinguish entries belonging to
    /// different saga types that share the same table
    /// </summary>
    public required string SagaType { get; set; }

    /// <summary>JSON-serialized saga state</summary>
    public required string StateJson { get; set; }

    /// <summary>
    /// <c>true</c> when the saga has been marked complete, completed sagas keep their state
    /// row but ignore new messages
    /// </summary>
    public bool IsCompleted { get; set; }

    /// <summary>
    /// Absolute UTC expiry of this entry, or <c>null</c> for no expiry
    /// </summary>
    public DateTimeOffset? ExpiresAtUtc { get; set; }
}
