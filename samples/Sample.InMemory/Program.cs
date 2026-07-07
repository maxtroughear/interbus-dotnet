using InterBus;
using InterBus.Extensions;
using InterBus.Generated;
using InterBus.InMemory.Extensions;
using InterBus.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Sample.AuditComponents;
using Sample.Components;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.AddOpenTelemetry(options => options
    .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("Sample.InMemory"))
    .AddOtlpExporter());

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService("Sample.InMemory"))
    .WithTracing(tracing => tracing
        .AddSource(InterBusTelemetry.ActivitySourceName)
        .AddOtlpExporter())
    .WithMetrics(metrics => metrics
        .AddMeter(InterBusTelemetry.MeterName)
        .AddOtlpExporter());

// Register InterBus with In-Memory transport
builder.Services.AddInterBus(bus =>
{
    bus.AddInMemoryTransport();

    bus.AddConsumer<GreetingConsumer>();
    bus.AddConsumer<FarewellConsumer>();
    bus.AddConsumer<EavesDroppingConsumer>();
    bus.AddConsumer<EchoConsumer>();
    bus.AddSaga<GreetingSaga, GreetingSagaState>(c => { c.UseInMemoryRepository(); });
});

var host = builder.Build();

// Start the host (this starts the InterBus background workers)
await host.StartAsync();

// Resolve the publisher and send a message
using (var scope = host.Services.CreateScope())
{
    var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
    var request = scope.ServiceProvider.GetRequiredService<IRequestClient<GreetingMessage, GreetingMessage>>();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();

    var message = new GreetingMessage("Hello from In-Memory Transport!");
    logger.LogInformation("Publishing message: {MessageText}", message.Text);

    await publisher.PublishAsync(message);

    await request.RequestAsync(message);
}

await Task.Delay(10_000);

await host.StopAsync();
