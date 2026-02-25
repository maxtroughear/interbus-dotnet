using System.Text.Json.Serialization;
using InterBus;
using InterBus.Extensions;
using InterBus.InMemory.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// This sample demonstrates InterBus with Native AOT.
// Key differences from the non-AOT sample:
//  1. A source-generated JsonSerializerContext lists every serializable message type,
//     replacing the reflection-based DefaultJsonTypeInfoResolver.
//  2. Consumers are registered manually via AddConsumer<TConsumer, T>() instead of
//     using the source generator (which targets netstandard2.0 and cannot be
//     AOT-published alongside a net10.0 executable).
//  3. bus.Options.SerializerOptions is set BEFORE any AddConsumer<> call so that
//     JsonTypeInfo<T> is resolved from the AOT-safe context at startup.

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddInterBus(bus =>
{
    // Switch to the source-generated serializer context before registering consumers.
    bus.Options.SerializerOptions = SampleAotJsonContext.Default.Options;

    bus.AddConsumer<GreetingConsumer, GreetingMessage>();
    bus.AddConsumer<FarewellConsumer, FarewellMessage>();
    bus.AddConsumer<EavesDroppingConsumer, GreetingMessage>();
    bus.AddConsumer<EavesDroppingConsumer, FarewellMessage>();

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
    public async Task Consume(GreetingMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
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
    public Task Consume(GreetingMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[EavesDroppingConsumer] Greeting: {message.Text}");
        return Task.CompletedTask;
    }

    public Task Consume(FarewellMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[EavesDroppingConsumer] Farewell: {message.Text}");
        return Task.CompletedTask;
    }
}

/// <summary>
/// Source-generated serialization context. Every message type used in this application
/// must have a corresponding [JsonSerializable] attribute so the AOT compiler can emit
/// the required metadata without runtime reflection.
/// </summary>
[JsonSerializable(typeof(GreetingMessage))]
[JsonSerializable(typeof(FarewellMessage))]
internal partial class SampleAotJsonContext : JsonSerializerContext
{
}
