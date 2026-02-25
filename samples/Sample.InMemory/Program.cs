using InterBus;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

// Register InterBus with In-Memory transport
builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();

    // Automatically register consumers discovered by the source generator
    bus.AddGeneratedConsumers();
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

public record GreetingMessage(string Text);

public record FarewellMessage(string Text);

// ReSharper disable ClassNeverInstantiated.Global
public class GreetingConsumer(IMessagePublisher publisher) : IConsumer<GreetingMessage>
{
    public async Task Consume(GreetingMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[GreetingConsumer] Received message: {message.Text}");

        var farewell = new FarewellMessage($"Goodbye from In-Memory Transport!");
        Console.WriteLine($"[GreetingConsumer] Publishing farewell message: {farewell.Text}");

        await publisher.Publish(farewell, cancellationToken);
    }
}

public class FarewellConsumer : IConsumer<FarewellMessage>
{
    public Task Consume(FarewellMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[FarewellConsumer] Received farewell message: {message.Text}");
        return Task.CompletedTask;
    }
}

public class EavesDroppingConsumer : IConsumer<GreetingMessage>, IConsumer<FarewellMessage>
{
    public Task Consume(GreetingMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[EavesDroppingConsumer] Received greeting message: {message.Text}");
        return Task.CompletedTask;
    }

    public Task Consume(FarewellMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[EavesDroppingConsumer] Received farewell message: {message.Text}");
        return Task.CompletedTask;
    }
}
