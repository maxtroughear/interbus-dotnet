namespace InterBus.Tests.InMemory;

// ---------------------------------------------------------------------------
// Test messages (top-level to avoid nested-type name mismatch between CLR and
// source-generator display strings, e.g. "A+B" vs "A.B")
// ---------------------------------------------------------------------------
public record OrderStarted(Guid OrderId, string CustomerEmail);
