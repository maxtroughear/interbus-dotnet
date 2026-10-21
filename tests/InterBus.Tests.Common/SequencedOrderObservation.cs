namespace InterBus.Tests.Common;

public sealed record SequencedOrderObservation(string MessageGroupId, int Sequence, DateTimeOffset ConsumedAt);
