namespace InterBus.PostgreSQL.Tests;

public sealed class GetOrderStatusConsumer(IPublisher publisher) : RequestReplyConsumer<GetOrderStatusRequest, GetOrderStatusReply>(publisher)
{
    protected override GetOrderStatusReply CreateReply(GetOrderStatusRequest message) => new(message.OrderId, "Shipped");
}
