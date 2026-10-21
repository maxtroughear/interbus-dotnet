namespace InterBus.PostgreSQL;

internal sealed partial class PostgreSqlLeaseMessagePump
{
    private sealed record ClaimedMessage(long MessageId, byte[] Payload, string HeadersJson, long PayloadId);
}
