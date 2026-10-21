using InterBus.MessagePumps;

namespace InterBus.Tests;

public partial class MessagePumpRuntimeTests
{
    private sealed class ScriptedSource(Delivery[] deliveries) : IBufferedSource
    {
        public int PrefetchCount => 4;

        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public async Task ReceiveAsync(DeliverySink sink, CancellationToken intakeToken)
        {
            foreach (var delivery in deliveries)
            {
                using var reservation = await sink.ReserveAsync(intakeToken);
                reservation.Handoff(delivery);
            }

            await Task.Delay(Timeout.Infinite, intakeToken);
        }
    }
}
