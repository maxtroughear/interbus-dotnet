namespace InterBus.AmazonSqs.Tests;

public sealed record SnsSerialisedConsumedMessage(OrderPlaced Message, MessageHeaders Headers);
