using InterBus.Dispatching;
using InterBus.MessagePumps;
using RabbitMQ.Client.Events;

namespace InterBus.RabbitMQ;

internal sealed partial class RabbitMqMessagePump
{
    private sealed class RabbitDelivery : IDelivery
    {
        private readonly RabbitMqMessagePump _owner;
        private readonly BasicDeliverEventArgs _args;
        private readonly int _generation;
        public ReadOnlyMemory<byte> Payload { get; }
        public MessageHeaders Headers { get; } = new();
        public Exception? DecodeError { get; }

        public RabbitDelivery(RabbitMqMessagePump owner, BasicDeliverEventArgs args)
        {
            _owner = owner;
            _args = args;
            _generation = Volatile.Read(ref owner._generation);
            Payload = args.Body.ToArray();
            try
            {
                Headers = RabbitMqHeaders.ParseAmqpHeaders(args.BasicProperties.Headers);
            }
            catch (Exception error)
            {
                DecodeError = error;
            }
        }

        public async Task<bool> SettleAsync(DispatchResult result, CancellationToken cancellationToken)
        {
            var channel = _owner._channel!;
            if (!channel.IsOpen || _generation != Volatile.Read(ref _owner._generation))
                return false;
            switch (result.Kind)
            {
                case DispatchResultKind.Success:
                    await channel.BasicAckAsync(_args.DeliveryTag, false, cancellationToken);
                    break;
                case DispatchResultKind.Release:
                    await channel.BasicNackAsync(_args.DeliveryTag, false, true, cancellationToken);
                    break;
                case DispatchResultKind.DeadLetter:
                    await _owner.DeadLetterAsync(channel, _owner._descriptor, _args, result, cancellationToken);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(result));
            }

            return _generation == Volatile.Read(ref _owner._generation);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
