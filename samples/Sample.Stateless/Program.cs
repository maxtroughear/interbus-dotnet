using InterBus;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sample.Components;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();

    bus.AddSaga<OrderSaga, OrderSagaState>(saga =>
    {
        saga.UseInMemoryRepository();
    });
});

var host = builder.Build();

await host.StartAsync();

using (var scope = host.Services.CreateScope())
{
    var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
    var orderId = Guid.NewGuid();

    Console.WriteLine("=== Stateless Saga Sample ===");
    Console.WriteLine();

    // Happy path: place → confirm → ship → deliver
    Console.WriteLine("--- Happy path ---");
    await publisher.PublishAsync(new PlaceOrder(orderId, "customer@example.com"));
    await Task.Delay(200);

    await publisher.PublishAsync(new ConfirmOrder(orderId));
    await Task.Delay(200);

    // ShipOrder and DeliverOrder are published automatically by the saga handlers above
    await Task.Delay(500);

    // Cancellation path: a separate order that gets cancelled
    Console.WriteLine();
    Console.WriteLine("--- Cancellation path ---");
    var cancelOrderId = Guid.NewGuid();
    await publisher.PublishAsync(new PlaceOrder(cancelOrderId, "other@example.com"));
    await Task.Delay(200);

    await publisher.PublishAsync(new CancelOrder(cancelOrderId));
    await Task.Delay(200);
}

await host.StopAsync();
