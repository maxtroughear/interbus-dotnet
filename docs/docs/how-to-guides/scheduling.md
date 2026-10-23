---
sidebar_position: 5
---

# Schedule messages

Inject `IScheduler` to defer a topic publication or a point-to-point send. Register scheduling
with `bus.AddScheduling(...)`, choose a delivery mode, and register a provider when that mode
requires one. Provider registration and delivery mode are separate choices.

## Choose a delivery mode

| Mode | Schedule ID returned to caller | Requirements |
| --- | --- | --- |
| `Directly()` | Provider ID | An `IMessageSchedulingProvider` in the caller's service collection |
| `ViaRequest()` | Provider ID through request/reply | Request/reply transport support and a scheduling consumer with a provider |
| `ViaPublish()` | `null` | A scheduling consumer with a provider; it publishes `ScheduleCreatedEvent` containing the ID |
| `OnTransport()` | `null` | Transport delay support; no separate provider |

Provider extensions include `AddTickerQScheduler()`, `AddHangfireScheduler()`,
`AddEventBridgeScheduler(...)`, and [AddAzureServiceBusScheduler()](./azure-service-bus-scheduling.md).
Configure the chosen provider's dependencies in the host.

For command-based scheduling, register `AddSchedulingConsumer()` through the scheduler builder
in the application processing the commands. This may be the caller or a dedicated service.
`ViaRequest()` waits for schedule creation; its `CancelAsync` still publishes a cancellation
command, as does `ViaPublish()`. Completion of those cancellation calls confirms command
publication, not that the provider has finished canceling the delivery.

`Directly()` serializes the message and calls the provider without running the publish pipeline.
`OnTransport()` runs the publish pipeline with a delay; whether that delay is honored depends
on the transport. It does not support cancellation and throws `NotSupportedException` from
`CancelAsync`.

## Schedule a publication or send

After configuring a provider and mode, resolve `IScheduler` from an application scope:

```csharp
var scheduler = scope.ServiceProvider.GetRequiredService<IScheduler>();
var message = new OrderPlaced(orderId, customerEmail, total);

var scheduleId = await scheduler.SchedulePublishAsync(
    message, DateTimeOffset.UtcNow.AddMinutes(10), cancellationToken);

await scheduler.ScheduleSendAsync<OrderPlacedConsumer, OrderPlaced>(
    message, TimeSpan.FromMinutes(10), cancellationToken);
```

`SchedulePublishAsync` fans out to consumers of the message type. `ScheduleSendAsync` targets
a consumer queue; overloads also accept a queue name or URI. Both support absolute timestamps
and relative delays, plus a configuration delegate for headers.

```csharp
await scheduler.SchedulePublishAsync(
    message,
    DateTimeOffset.UtcNow.AddMinutes(10),
    context => context.Headers["source"] = "billing",
    cancellationToken);
```

## Cancel a schedule

Keep the returned ID unchanged. Its format and cancellation guarantees belong to the provider.
A provider may restrict IDs to a particular namespace, account, or storage configuration.

```csharp
if (scheduleId is not null)
    await scheduler.CancelAsync(scheduleId, cancellationToken);
```

With `ViaPublish()`, obtain the provider ID from `ScheduleCreatedEvent` before requesting
cancellation. Cancellation can race delivery; check the provider's limits and error behavior.
Canceling an operation with a `CancellationToken` does not retract an already accepted schedule.

For native Azure scheduling and managed identity configuration, see
[Schedule with Azure Service Bus](./azure-service-bus-scheduling.md).
