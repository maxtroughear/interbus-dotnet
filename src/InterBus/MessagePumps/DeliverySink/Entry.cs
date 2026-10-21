namespace InterBus.MessagePumps;

public sealed partial class DeliverySink
{
    internal sealed record Entry(IDelivery Delivery, SemaphoreSlim Capacity);
}
