using InterBus;
using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;
using InterBus.Generated;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sample.AuditComponents;
using Sample.Components;

var builder = Host.CreateApplicationBuilder(args);

// Register InterBus with In-Memory transport
builder.Services.AddInterBus(bus =>
{
    bus.AddAzureServiceBusTransport(options =>
    {
        options.ConnectionString = builder.Configuration["AzureServiceBus:ConnectionString"] ??
                                   throw new InvalidOperationException(
                                       "Azure Service Bus connection string not configured. Set AzureServiceBus:ConnectionString in appsettings.json or the AZURESERVICEBUS__CONNECTIONSTRING environment variable");
    });

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
    var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

    var message = new GreetingMessage("Hello from AzureServiceBus Transport!");
    Console.WriteLine($"[Publisher] Publishing message: {message.Text}");

    await publisher.PublishAsync(message);
}

await Task.Delay(10000);

await host.StopAsync();
