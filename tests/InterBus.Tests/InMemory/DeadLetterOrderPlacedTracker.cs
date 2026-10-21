using InterBus.DeadLettering;

namespace InterBus.Tests.InMemory;

/// <summary>Tracks dead-lettered <see cref = "OrderPlaced"/> messages for integration tests.</summary>
public sealed class DeadLetterOrderPlacedTracker : MessageTracker<DeadLetter<OrderPlaced>>;
