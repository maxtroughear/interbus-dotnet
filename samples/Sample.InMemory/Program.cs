using InterBus;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sample.AuditComponents;
using Sample.Components;

var builder = Host.CreateApplicationBuilder(args);

// Register InterBus with In-Memory transport
builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();

    bus.AddConsumer<GreetingConsumer>();
    bus.AddConsumer<FarewellConsumer>();
    bus.AddConsumer<EavesDroppingConsumer>();
});

var host = builder.Build();

// Start the host (this starts the InterBus background workers)
await host.StartAsync();

// Resolve the publisher and send a message
using (var scope = host.Services.CreateScope())
{
    var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

    var message = new GreetingMessage("Hello from In-Memory Transport!");
    Console.WriteLine($"[Publisher] Publishing message: {message.Text}");

    await publisher.Publish(message);
}

await Task.Delay(1000);

await host.StopAsync();
