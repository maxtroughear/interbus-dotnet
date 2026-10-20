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
| 8 | `41f93f6` | Remove prototype pumps, strengthen readiness/recovery/cleanup tests and document behavior |

## Final behavior

Every registered consumer uses core scoped processing, settlement outcome handling and lifecycle supervision. SQS and PostgreSQL lease mode share one receive ownership budget and concurrency limit per consumer, regardless of receive pump count. Managed deliveries remain inside their SDK callback or owning transaction through settlement. In-memory uses fixed core workers directly over its existing broker channel.

Intake cancellation is separate from processing cancellation. SDK startup readiness is awaited before host startup completes. Shutdown stops intake, drains accepted work with ownership resources alive, and cancels processing at the host deadline. Faults are reported promptly while cleanup finishes. Controller collections are snapshots, and multiple shutdown failures are aggregated after all consumers are stopped.

Settlement failures never trigger an automatic conflicting second settlement. Rejected tokens and lost locks are reported as rejected settlement. SQS renewal is stopped and awaited during cleanup, including failed handoff and failed delete/send. PostgreSQL lease SQL retains token/expiry checks and delayed release. Transactional mode establishes ambient state in the same async flow as core processing and restores it in `finally`.

FIFO SQS groups serialize through settlement. Azure keeps one call per session and concurrent independent sessions. RabbitMQ retains SDK QoS and callback scheduling, adds a shared dispatch gate across connection generations, and rejects settlement for an earlier connection generation. Callback buffers remain within their delivery lifetime.

## Validation

All affected test projects build. Final relevant suite results:

| Project | Passed | Skipped | Scope |
| --- | ---: | ---: | --- |
| InterBus.Tests | 178 | 0 | Entire core/in-memory suite |
| InterBus.PostgreSQL.Tests | 55 | 0 | All except high-volume `ConcurrencyTests`, PostgreSQL 17/18 and both modes |
| InterBus.AmazonSqs.Tests | 29 | 1 | All except high-volume `ConcurrencyTests` and the separate `EventBridgeSchedulerTests` fixture |
| InterBus.AzureServiceBus.Tests | 21 | 0 | All except high-volume `ConcurrencyTests`; includes the new `SessionConcurrencyTests` |
| InterBus.RabbitMQ.Tests | 8 | 0 | All except high-volume `ConcurrencyTests` |

Commands use `dotnet test tests/<project>/<project>.csproj --no-restore -v quiet`, with `--filter 'FullyQualifiedName!~ConcurrencyTests'` for transport parity runs. Azure uses the precise exclusion `FullyQualifiedName!~InterBus.AzureServiceBus.Tests.ConcurrencyTests` so session scheduling coverage runs. SQS additionally excludes `FullyQualifiedName~EventBridgeSchedulerTests`. `--no-build` was used for independent integration runs after building their projects.

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

## Follow-up failure and concurrency coverage

All seven scenarios identified in the implementation review now have tests:

| Scenario | Added coverage and result |
| --- | --- |
| A later stop caller supplies a shorter deadline | Deterministic lifecycle tests cover an original unlimited or longer stop, retained active resources, and a real host stopping after controller shutdown began. The two deadline regressions failed before the fix. Every caller's deadline now cancels processing while shared cleanup continues. |
| SQS loses renewal ownership while settlement is blocked | Three controlled SDK races cover acknowledge, dead-letter send and dead-letter delete. All failed before the fix. Settlement now reports rejection if ownership is lost during the operation, and dead-letter send does not proceed to delete after known loss. |
| RabbitMQ recovers or stops during an outage | Recovery tests wait for all logical messages and zero ready/unacknowledged broker depth, check rejected stale settlements, and enforce the concurrency bound across connection generations. A broker-stop test verifies deadline cancellation and callback completion. |
| One PostgreSQL notification should wake multiple idle transaction workers | A single transaction emits one coalesced notification for three messages while three workers wait with a 30-second fallback. Tests failed on PostgreSQL 17 and 18 before the fix. Versioned broadcast notifications now wake all idle workers and cover notification arrival between claim and wait. |
| PostgreSQL settlement fails at different transaction stages | PostgreSQL 17/18 exercise acknowledgement SQL failure, deferred commit failure, backend termination and a test TCP proxy dropping the actual COMMIT response. Assertions cover atomic rollback/redelivery or committed ambiguous outcome, recovered subsequent dispatch, ambient transaction cleanup, payload cleanup and closed test-owned connections. |
| Several consumers fault together or during drain | Real host tests observe every retained exception and aggregated shutdown failures. Host observation tasks now capture errors as results so later consumer faults cannot become unobserved task exceptions after the first fault is surfaced. |
| Azure sessions have cancellation, lock-loss or startup failures | SDK adapter tests cover all outcomes during stop, lock loss before/during drain and partially initialized session processor cleanup. An emulator integration test runs two independent sessions concurrently while preserving one callback per session and message order. |

The follow-up adds 26 test cases across the five projects, including the existing RabbitMQ recovery cases strengthened with settlement assertions. Transport integration tests use the current production registrations; deterministic adapter tests isolate races through controlled SDK calls. PostgreSQL fault injection and Azure/RabbitMQ scheduling and outage tests use actual containers/emulators. Real AWS credentials remain unavailable, so the existing AWS-only test remains skipped; no real cloud outage or twelve-hour renewal claim is made.
