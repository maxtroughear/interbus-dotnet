namespace InterBus.Tests;

// Types used only by FifoConfigurationTests, defined at namespace level to avoid
// source-generator accessibility issues with nested private types.
public record FifoTestOrderPlaced(Guid OrderId, string CustomerId);
