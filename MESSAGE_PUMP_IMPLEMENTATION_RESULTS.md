# Message pump implementation results

Implemented on `maxtroughear-test-cleanup-plan`, starting from `f14bf59`. The checkout was initially detached and clean; it was switched to the requested branch before edits.

## Phase commits

| Phase | Commit | Result |
| --- | --- | --- |
| 1 | `e270a25` | Shared owned delivery, scoped processing, reservations and per-consumer runtime |
| 2 | `e0177a9` | Core host, lifecycle supervision, fault state and graceful shutdown |
| 3 | `97103eb` | Activate SQS buffered execution |
| 4 | `be847f8` | PostgreSQL lease delivery and shared receive budget |
| 5 | `cf719af` | PostgreSQL transaction-owning managed execution |
| 6 | `f8c8a5b` | Azure normal/session managed callbacks |
| 7 | `3879e3c` | RabbitMQ managed callbacks and in-memory core workers |
| 8 | Final cleanup commit | Remove prototype pumps, strengthen readiness/recovery/cleanup tests and document behavior |

## Final behavior

Every registered consumer uses core scoped processing, settlement outcome handling and lifecycle supervision. SQS and PostgreSQL lease mode share one receive ownership budget and concurrency limit per consumer, regardless of receive pump count. Managed deliveries remain inside their SDK callback or owning transaction through settlement. In-memory uses fixed core workers directly over its existing broker channel.

Intake cancellation is separate from processing cancellation. SDK startup readiness is awaited before host startup completes. Shutdown stops intake, drains accepted work with ownership resources alive, and cancels processing at the host deadline. Faults are reported promptly while cleanup finishes. Controller collections are snapshots, and multiple shutdown failures are aggregated after all consumers are stopped.

Settlement failures never trigger an automatic conflicting second settlement. Rejected tokens and lost locks are reported as rejected settlement. SQS renewal is stopped and awaited during cleanup, including failed handoff and failed delete/send. PostgreSQL lease SQL retains token/expiry checks and delayed release. Transactional mode establishes ambient state in the same async flow as core processing and restores it in `finally`.

FIFO SQS groups serialize through settlement. Azure keeps one call per session and concurrent independent sessions. RabbitMQ retains SDK QoS and callback scheduling, adds a shared dispatch gate across connection generations, and rejects settlement for an earlier connection generation. Callback buffers remain within their delivery lifetime.

## Validation

All affected test projects build. Final relevant suite results:

| Project | Passed | Skipped | Scope |
| --- | ---: | ---: | --- |
| InterBus.Tests | 173 | 0 | Entire core/in-memory suite |
| InterBus.PostgreSQL.Tests | 45 | 0 | All except high-volume `ConcurrencyTests`, PostgreSQL 17/18 and both modes |
| InterBus.AmazonSqs.Tests | 26 | 1 | All except high-volume `ConcurrencyTests` and the separate `EventBridgeSchedulerTests` fixture |
| InterBus.AzureServiceBus.Tests | 14 | 0 | All except high-volume `ConcurrencyTests` |
| InterBus.RabbitMQ.Tests | 7 | 0 | All except high-volume `ConcurrencyTests` |

Commands use `dotnet test tests/<project>/<project>.csproj --no-restore -v quiet`, with `--filter 'FullyQualifiedName!~ConcurrencyTests'` for transport parity runs. SQS additionally excludes `FullyQualifiedName~EventBridgeSchedulerTests`. `--no-build` was used for independent integration runs after building their projects.

Deterministic additions cover shared receive capacity/concurrency, fresh scopes and disposal, dispatch outcomes, rejected and ambiguous settlement, ordering through settlement, canceled processing, initialization and partial startup failures, readiness cancellation, repeated stop, shutdown deadlines, prompt background fault reporting, snapshot stability and aggregated cleanup failures.

Transport additions cover SQS registered host execution, raw/SNS envelopes, blocked-buffer renewal, FIFO progress across groups, failed delete/send, malformed envelopes, invalid receipts/renewal, cancellation after batch receive, FIFO dead-letter diagnostics and topology retry ordering; PostgreSQL strict concurrency with multiple receive sources, forced listener restart during blocked handlers, restored transaction access/isolation and rollback before redelivery; Azure callback lifetime through SDK stop cancellation, result mapping, lock loss and recoverable SDK errors; RabbitMQ forced connection recovery during blocked callbacks and shutdown before channel disposal; and in-memory configured worker counts through a real host.

Existing publish/consume, dead-letter and request/reply coverage runs through migrated registrations. The PostgreSQL dead-letter fixture previously used a consumer that always succeeded; it now throws and waits for the actual database outcome. Lease stall fixtures are restricted to lease mode. PostgreSQL high-volume tests ran separately during the lease migration, rather than serving as the concurrency-bound assertion.

## Deliberate changes and limits

- `WithPumpCount` requires a positive count. Managed transports and in-memory reject counts above one. In-memory now honors `ConcurrencyCount`.
- SQS rejects negative prefetch, invalid polling, visibility below one second or above twelve hours, and renewal intervals outside the positive interval below visibility. Zero prefetch remains supported.
- A failed SQS renewal marks ownership uncertain and rejects subsequent settlement. Default release still lets visibility expire.
- SQS transient topology failures now use the configured connection retry policy before intake. PostgreSQL connection/shutdown SQLSTATEs are classified as recoverable; unrelated SQL failures still surface.
- Prototype pump contracts and obsolete transport-local processing implementations were removed. Virtual request/reply receive loops remain intact.
- The skipped SQS test requires real AWS credentials. Container/emulator infrastructure was available for the completed runs. Real cloud long-duration lock renewal and outage behavior were not independently validated.
- SQS, Azure and RabbitMQ high-volume benchmarks and the separate EventBridge scheduler fixture were outside the final parity run. No throughput claims are made.
- RabbitMQ now owns a dedicated consume connection per consumer runtime; the publish connection remains shared. This increases connection count with multiple consumers and makes disposal ownership explicit.
- In-memory queued envelopes are not durable and are not drained after intake stops. A handler ignoring cancellation can exceed the shutdown budget; resources remain alive until its work exits, and shutdown reports incomplete work.

User-facing execution, ordering and shutdown documentation is in `docs/docs/reference/message-pumps.md`.
