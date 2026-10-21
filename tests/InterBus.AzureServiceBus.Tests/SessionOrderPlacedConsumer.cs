namespace InterBus.AzureServiceBus.Tests;

public sealed class SessionOrderPlacedConsumer(SessionOrderPlacedTracker tracker) : TrackingConsumer<SessionOrderPlaced, SessionOrderPlacedTracker>(tracker);
