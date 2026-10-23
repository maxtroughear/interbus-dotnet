---
sidebar_position: 6
---

# Schedule with Azure Service Bus

Use the native scheduler from `InterBus.AzureServiceBus` to schedule queue or topic messages
and cancel them using broker sequence numbers. It uses the transport's connection and cached
senders, with no scheduler database or delivery worker.

## Register the transport and scheduler

```csharp
using InterBus.AzureServiceBus.Extensions;
using InterBus.Extensions;

builder.Services.AddInterBus(bus =>
{
    bus.AddAzureServiceBusTransport(options =>
    {
        options.ConnectionString = connectionString;
    });
    bus.AddScheduling(scheduler => scheduler.AddAzureServiceBusScheduler().Directly());
});
```

Register application consumers as usual. Target queues must exist before scheduling; topic
destinations are ensured through the transport topology builder. Queue prefixes, topic naming,
headers, and session routing follow the transport's conventions.

Use `IScheduler.SchedulePublishAsync` for fan-out or `ScheduleSendAsync` for queue delivery.
See [Schedule messages](./scheduling.md) for API examples and delivery modes. To use request/reply
for schedule creation in the same application, replace the scheduling registration with:

```csharp
bus.AddScheduling(scheduler => scheduler
    .AddAzureServiceBusScheduler()
    .ViaRequest()
    .AddSchedulingConsumer());
```

`ViaPublish()` with the consumer returns null inline and exposes the provider ID through
`ScheduleCreatedEvent`. Both command modes publish cancellation commands asynchronously.

## Authenticate an Azure-hosted application

The transport accepts any Azure `TokenCredential`, including managed identity credentials for
App Service, Functions, Container Apps, and other hosts supporting managed identity.
Enable a system-assigned identity on the host, or attach a user-assigned identity. Grant that
identity access to the Service Bus namespace. **Azure Service Bus Data Owner** allows the
transport to create topology and send/receive messages; scope it to the application namespace.
A send-only or receive-only role is insufficient when the application also declares topology.

Configure the namespace host name and credential instead of connection strings:

```csharp
using Azure.Identity;

bus.AddAzureServiceBusTransport(options =>
{
    options.FullyQualifiedNamespace = "example.servicebus.windows.net";
    options.Credential = new ManagedIdentityCredential();
});
```

For a user-assigned identity, select its client ID explicitly:

```csharp
options.Credential = new ManagedIdentityCredential(
    ManagedIdentityId.FromUserAssignedClientId(managedIdentityClientId));
```

The identity must be attached to the host; creating it alone does not make it available there.
For local development, `DefaultAzureCredential` can use a developer login such as `az login`.
For AKS workload identity, configure the cluster's federated identity and supply a
`WorkloadIdentityCredential` (or a credential chain configured to use it).

The host owns the credential's lifetime. The transport reuses it for messaging and administration
and owns/disposes its SDK clients. When `FullyQualifiedNamespace` is set, `Credential` is required
and both connection-string settings are ignored. This configuration is independent of the
`AzureCliCredential` used by the repository's GitHub Actions tests.

See Microsoft's guidance for [system-assigned identities](https://learn.microsoft.com/en-us/dotnet/azure/sdk/authentication/system-assigned-managed-identity),
[user-assigned identities](https://learn.microsoft.com/en-us/dotnet/azure/sdk/authentication/user-assigned-managed-identity),
and [Service Bus access roles](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-managed-service-identity).

## Store IDs and cancel deliveries

With `Directly()`, the schedule call returns an opaque ID containing the physical destination
and the broker sequence number. Store it unchanged and pass it to `IScheduler.CancelAsync` using
the same Service Bus namespace. Another process can cancel the schedule without a local registry,
and changing a naming prefix does not change the destination encoded in an existing ID.

Cancellation cannot recall an activated message or guarantee success when racing activation.
SDK failures propagate; cancellation of a missing schedule is not promised to be idempotent.
Messages scheduled for an already-due timestamp still use native scheduling and return an ID.

`OnTransport()` remains an alternative for transport delays, but returns no schedule ID and does
not support cancellation. Emulator queue cancellation has a [known bug](https://github.com/Azure/azure-service-bus-emulator-installer/issues/119);
use a fixed emulator image or a real namespace to validate that behavior.
