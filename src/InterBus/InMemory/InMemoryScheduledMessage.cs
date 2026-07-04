namespace InterBus.InMemory;

public class InMemoryScheduledMessage(
    string scheduleId,
    Uri destination,
    byte[] payload,
    MessageHeaders headers,
    DateTimeOffset scheduledAt)
{
    public string ScheduleId { get; } = scheduleId;
    public Uri Destination { get; } = destination;
    public byte[] Payload { get; } = payload;
    public MessageHeaders Headers { get; } = headers;
    public DateTimeOffset ScheduledAt { get; } = scheduledAt;
}
