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
    bus.AddConsumer<GreetingConsumer>(c =>
    {
        c
            .Consumes<GreetingConsumer, GreetingMessage>()
            .Dispatch((context, pipeline, sp, ct) =>
            {
                if (!context.Headers.TryGetValue("x-type", out var msgType))
                    throw new InvalidOperationException(
                        $"Message received without x-type header for consumer {context.Descriptor.ConsumerType.FullName}");

                return msgType switch
                {
                    "greeting" => pipeline.ExecuteAsync<GreetingConsumer, GreetingMessage>(context, ct),
                    "test" => pipeline.ExecuteAsync<GreetingConsumer, FarewellMessage>(context, ct),
                    _ => throw new InvalidOperationException($"Unknown message type: {msgType}")
                };
            });
    });
    bus.AddSaga<GreetingSaga, GreetingSagaState>(c =>
    {
        c.Consumes<GreetingSaga, GreetingMessage>();
        c.Dispatch((context, pipeline, sp, ct) =>
        {
            return context.Headers["x-type"] switch
            {
                "test" => pipeline.ExecuteAsync<FarewellMessage, GreetingMessage>(context, ct),
                _ => pipeline.ExecuteAsync<GreetingSaga, GreetingMessage>(context, ct)
            };
        });
    });
});

var host = builder.Build();

// Start the host (this starts the InterBus background workers)
await host.StartAsync();

// Resolve the publisher and send a message
using (var scope = host.Services.CreateScope())
{
    var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

    var message = new GreetingMessage("Hello from In-Memory Transport!");
    Console.WriteLine($"[Publisher] Publishing message: {message.Text}");

    await publisher.PublishAsync(message);
}

await Task.Delay(1000);

await host.StopAsync();
