using InterBus.Dispatching;
using InterBus.MessagePumps;

namespace InterBus.PostgreSQL;

internal sealed partial class PostgreSqlLeaseMessagePump
{
    private sealed class LeaseDelivery : IDelivery
    {
        private readonly PostgreSqlLeaseMessagePump _owner;
        private readonly ClaimedMessage _claim;
        private readonly string _token;
        private readonly CancellationTokenSource _renewal = new();
        private readonly Task<bool> _renewalTask;
        public ReadOnlyMemory<byte> Payload => _claim.Payload;
        public MessageHeaders Headers { get; } = new();
        public Exception? DecodeError { get; }

        public LeaseDelivery(PostgreSqlLeaseMessagePump owner, ClaimedMessage claim, string token)
        {
            _owner = owner;
            _claim = claim;
            _token = token;
            _renewalTask = owner.RunLeaseRenewalLoopAsync(claim.MessageId, owner.Descriptor.QueueName, token, _renewal.Token);
            try
            {
                Headers = PostgreSqlTransport.DeserializeHeaders(claim.HeadersJson);
            }
            catch (Exception error)
            {
                DecodeError = error;
            }
        }

        public Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken) => result.Kind switch
        {
            DispatchResultKind.Success => _owner.AckClaimAsync(_claim.MessageId, _claim.PayloadId, _token, cancellationToken),
            DispatchResultKind.DeadLetter => _owner.DeadLetterClaimAsync(_claim.MessageId, _claim.PayloadId, _token, _owner.Descriptor.QueueName, result, cancellationToken),
            DispatchResultKind.Release => _owner.ReleaseClaimAsync(_claim.MessageId, _owner.Descriptor.QueueName, _token, _owner.Options.FailureRedeliveryDelay, cancellationToken),
            _ => throw new ArgumentOutOfRangeException(nameof(result))
        };
        public async ValueTask DisposeAsync()
        {
            await _renewal.CancelAsync();
            try
            {
                await _renewalTask;
            }
            finally
            {
                _renewal.Dispose();
            }
        }
    }
}
