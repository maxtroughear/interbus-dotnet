using System.Text.Json.Serialization;
using InterBus;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddInterBus(bus =>
{
    // Use source-generated serializer context before registering consumers
    bus.UseSystemTextJsonSerializer(SampleAotJsonContext.Default.Options);

    bus.AddConsumer<GreetingConsumer>();
    bus.AddConsumer<FarewellConsumer>();
    bus.AddConsumer<EavesDroppingConsumer>();

    bus.AddInMemoryTransport();
});

var host = builder.Build();

await host.StartAsync();

using (var scope = host.Services.CreateScope())
{
    var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

    var message = new GreetingMessage("Hello from Native AOT!");
    Console.WriteLine($"[Publisher] Publishing: {message.Text}");

    await publisher.Publish(message);
}

// Allow the background pump to deliver the messages before stopping.
await Task.Delay(500);

await host.StopAsync();

public record GreetingMessage(string Text);

public record FarewellMessage(string Text);

public class GreetingConsumer(IMessagePublisher publisher) : IConsumer<GreetingMessage>
{
    public async Task Consume(GreetingMessage message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[GreetingConsumer] Received: {message.Text}");
        await publisher.Publish(new FarewellMessage("Goodbye from Native AOT!"), cancellationToken);
    }
}

public class FarewellConsumer : IConsumer<FarewellMessage>
{
    public Task Consume(FarewellMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[FarewellConsumer] Received: {message.Text}");
        return Task.CompletedTask;
    }
}

public class EavesDroppingConsumer : IConsumer<GreetingMessage>, IConsumer<FarewellMessage>
{
    public Task Consume(FarewellMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[EavesDroppingConsumer] Farewell: {message.Text}");
        return Task.CompletedTask;
    }

    public Task Consume(GreetingMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[EavesDroppingConsumer] Greeting: {message.Text}");
        return Task.CompletedTask;
    }
}

/// <summary>
/// Source-generated serialization context. Every message type used in this application
/// must have a corresponding [JsonSerializable] attribute, so the AOT compiler can emit
/// the required metadata without runtime reflection.
/// </summary>
[JsonSerializable(typeof(GreetingMessage))]
[JsonSerializable(typeof(FarewellMessage))]
internal partial class SampleAotJsonContext : JsonSerializerContext
{
}
