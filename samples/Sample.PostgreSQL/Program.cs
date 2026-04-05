using System.Text.Json.Serialization;
using InterBus;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sample.AuditComponents;
using Sample.Components;

var builder = Host.CreateApplicationBuilder(args);

// Read the PostgreSQL connection string from configuration.
// Set the "ConnectionStrings:InterBus" key in appsettings.json or via the
// CONNECTIONSTRINGS__INTERBUS environment variable when running the sample.
var connectionString = builder.Configuration["ConnectionStrings:InterBus"]
    ?? throw new InvalidOperationException(
        "PostgreSQL connection string not configured. " +
        "Set ConnectionStrings:InterBus in appsettings.json or the " +
        "CONNECTIONSTRINGS__INTERBUS environment variable.");

// Register InterBus with the PostgreSQL transport.
// The transport automatically creates the required schema and tables on first run.
builder.Services.AddInterBus(bus =>
{
    // Use source-generated serializer context for Native AOT compatibility.
    bus.UseSystemTextJsonSerializer(SamplePostgreSqlJsonContext.Default.Options);

    bus.AddPostgreSqlTransport(o => o.ConnectionString = connectionString);

    bus.AddConsumer<GreetingConsumer>();
    bus.AddConsumer<FarewellConsumer>();
    bus.AddConsumer<EavesDroppingConsumer>();
});

var host = builder.Build();

// Start the host (this starts the InterBus background workers and schema migration)
await host.StartAsync();

// Resolve the publisher and send a message
using (var scope = host.Services.CreateScope())
{
    var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

    var message = new GreetingMessage("Hello from PostgreSQL Transport!");
    Console.WriteLine($"[Publisher] Publishing message: {message.Text}");

    await publisher.Publish(message);
}

// Allow time for the consumers to process the messages
await Task.Delay(2000);

await host.StopAsync();

/// <summary>
/// Source-generated serialization context. Every message type used in this application
/// must have a corresponding [JsonSerializable] attribute, so the AOT compiler can emit
/// the required metadata without runtime reflection.
/// </summary>
[JsonSerializable(typeof(GreetingMessage))]
[JsonSerializable(typeof(FarewellMessage))]
internal partial class SamplePostgreSqlJsonContext : JsonSerializerContext
{
}
