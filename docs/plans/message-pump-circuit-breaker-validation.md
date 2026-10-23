# Message pump circuit breaker validation

Validated locally on 23 October 2026 with .NET SDK 10.0.402 and Docker 29.9.2.

## Implementation

`UsePumpCircuitBreaker` configures one coordinator per consumer lifetime, using the
existing source factory and execution models. Dependency and transport policy state
is independent of pump lifecycle state. Admission, generation attribution, bounded
probes and ownership drain are shared across workers. Accepted deliveries retain
processing tokens and resources through settlement and cleanup, even after a circuit
opens or a probe deadline expires.

Internal stage records distinguish acquisition classification/retry decisions,
decode, final dispatch, settlement, cleanup, surrounding scope, deadlines and final
completion. Settlement exceptions remain absorbed by the existing processor; their
structured evidence prevents successful recovery. Application predicates and
observers cannot change delivery handling. Transitions and diagnostic failures are
logged; `PumpLifetime.Circuits` supplies policy snapshots.

RabbitMQ uses manual-ack `BasicGetAsync` while the ordinary subscription is canceled.
Azure normal probes retain the stopped processor and use standalone receivers.
Azure session probes drain and dispose the old processor before accepting a session,
renew its lock, dispose the receiver after processing and create a fresh processor
when recovery completes. Unsupported callback capabilities fail startup.

## Automated results

| Coverage | Result |
| --- | --- |
| Core suite, including public-contract circuit tests | 226 passed |
| RabbitMQ circuit and managed shutdown tests | 7 passed |
| Azure normal/session circuit, probe adapter and existing managed adapter tests | 19 passed |
| PostgreSQL suite, including new circuit and circuit-enabled settlement fault cases | 74 passed |
| SQS message pump adapter tests, including disabled/closed throughput variants | 19 passed |

The solution builds. A clean compilation reports only the three existing nullable
warnings in `EventBridgeDeliveryTests` and PostgreSQL `RequestReplyTests`.
`git diff --check` passes.

Core tests use a manual `TimeProvider` and explicit processing/settlement barriers.
They cover shared probe admission, accepted buffered drain, callback pause without
self-deadlock, generation isolation, settlement rejection, cleanup/scope failure,
probe timeout without premature disposal, policy composition, empty probes,
shutdown, predicate failures, configuration conflicts, retry exhaustion and combined
retry/circuit scheduling. A queued callback resume rechecks circuit eligibility
inside the serialized control gate.

RabbitMQ tests use `rabbitmq:4-alpine` with backlog and concurrency eight, inspect
broker consumer counts while a recovery delivery is blocked, and exercise connection
recovery while the subscription is canceled. Recovery dispatches the backlog once
without restoring an ordinary subscription during probing. Existing managed tests
also exercise accepted callback drain and channel ownership.

Azure tests use the actual 7.21.0 SDK, Service Bus emulator and SQL Server container.
Both normal and session recovery hold the probe for seven seconds on a queue with a
five-second lock, then verify settlement, backlog completion and session ordering.
Adapter tests additionally cover one receive per grant, disabled prefetch, empty
receives, lock-renewal loss, receiver cleanup, retained normal processors and fatal
configuration errors.

PostgreSQL tests run transactional and lease modes against PostgreSQL 17 and 18.
Circuit-specific tests block the first recovery delivery and verify backlog stays
paused. Transactional fault injection includes a rejected commit and a dropped
successful commit response with the circuit enabled. An ambiguous commit is not
replayed and does not recover a policy requiring settlement evidence; a subsequent
successful delivery can recover it.

Useful commands (all run from the repository root):

```sh
dotnet build InterBus.slnx --no-restore -v quiet
dotnet test tests/InterBus.Tests/InterBus.Tests.csproj --no-build -v minimal
dotnet test tests/InterBus.PostgreSQL.Tests/InterBus.PostgreSQL.Tests.csproj --no-build -v minimal
dotnet test tests/InterBus.RabbitMQ.Tests/InterBus.RabbitMQ.Tests.csproj --no-build --filter 'FullyQualifiedName~PumpCircuitTests|FullyQualifiedName~ManagedShutdownTests' -v minimal
dotnet test tests/InterBus.AzureServiceBus.Tests/InterBus.AzureServiceBus.Tests.csproj --no-build --filter 'FullyQualifiedName~PumpCircuitTests|FullyQualifiedName~ProbeAdapterTests|FullyQualifiedName~ManagedAdapterTests|FullyQualifiedName~SessionAdapterTests' -v minimal
dotnet test tests/InterBus.AmazonSqs.Tests/InterBus.AmazonSqs.Tests.csproj --no-build --filter 'FullyQualifiedName~MessagePumpAdapterTests' -v minimal
```

## Review follow-up

Policy configuration now uses symmetric `Dependency(...)` and `Transport(...)`
methods, with explicit `DisableDependency()` and `DisableTransport()` methods.
Application configuration no longer needs nullable policy access or policy object
construction. The complete solution builds after migrating its configuration calls;
the existing core suite includes 29 circuit behavior and configuration tests using this API.

Circuit compatibility validation inspects keyed middleware registrations from the
resilience extensions and the completed bus registration. The generic consumer
builder carries no circuit-specific checks or dispatch-circuit flag. The two breaker
features remain mutually exclusive on one consumer because dispatch-only admission
can reject accepted drain work and recovery probes. Direct generic middleware
registration is also validated after configuration.

The circuit tests now live in `InterBus.Tests` and use only public consumer
registration, source contracts, `PumpController`, `PumpLifetime` and policy snapshots.
The core friend-assembly declaration and separate pump test project have been
removed. Direct coordinator and typed-observation assertions were replaced with
observable acquisition, settlement, cleanup, failure and lifecycle behavior.
No runtime type or member was made public for testing. The source generator is
unchanged.

The public-contract tests also exposed and now cover an idle-wait registration race:
when a circuit opens after a receive but before its deferred idle wait is installed,
core rechecks admission instead of waiting indefinitely for a source notification.
The full core suite and affected polling adapter tests were rerun after the fix.

## SQS steady state

The fixed-latency adapter benchmark processes 512 messages with eight handlers,
three receive pumps, a batch limit of ten, 2 ms receive latency and 5 ms handler work.
The same fake SQS fixture supplies both configurations.

| Circuit | Prefetch | Elapsed | Receives | Maximum owned | Handler utilization |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disabled | 0 | 511.4 ms | 439 | 8 | 63.6% |
| Closed | 0 | 498.2 ms | 419 | 8 | 66.2% |
| Disabled | 32 | 347.0 ms | 248 | 40 | 97.4% |
| Closed | 32 | 345.4 ms | 250 | 40 | 97.1% |

Ownership remains bounded by concurrency plus prefetch. These measurements show no
unexplained regression in this fixture; they are not a real AWS throughput claim.

## Validation limits

Broker/database infrastructure was available locally. Callback support was exercised
with real SDKs and containers, in addition to fake-source tests. Live Azure service
failover, production network behavior and AWS-hosted SQS performance were not tested.
The emulator results do not establish all live Azure retry/failover guarantees.
Cross-instance probe coordination remains outside this feature's scope.
