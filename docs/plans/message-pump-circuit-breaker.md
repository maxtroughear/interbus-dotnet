# Message pump circuit breaker proposal

Status: feature proposal for [issue 210](https://github.com/maxtroughear/interbus-dotnet/issues/210), following the [pump core refactor](message-pump-core-refactor.md).

## Outcome and scope

Temporarily suspend one consumer's intake during a dependency or transport outage, then recover through controlled probes. Avoid repeated broker receives/releases while preserving accepted-delivery ownership, concurrency, ordering and settlement.

Circuit state belongs to one active consumer descriptor runtime. All of its buffered receive pumps, scoped workers and callbacks share it. Separate application instances have independent circuits; this feature does not globally pause a queue or coordinate distributed probes. Shared-dependency circuits across consumers are future work.

Keep one `PumpLifetime` initialized and supervised through open/half-open periods. Circuit state is separate from running/stopping/faulted lifecycle state. Terminal failures still fault the host; opening a circuit is not a source failure.

## Configuration

Add an opt-in consumer builder method, tentatively `UsePumpCircuitBreaker`. Leave existing consumers unchanged unless enabled. Configure dependency and transport policies separately, feeding one intake coordinator:

| Policy | Observations | Recovery evidence |
| --- | --- | --- |
| Dependency | Classified final dispatch outcomes | A real probe delivery reaches the required successful completion boundary |
| Transport | Classified acquisition failures and optionally settlement failures | Successful acquisition operation; optionally required successful settlement |

Intake is permitted only when every enabled policy permits it. A single acquisition can serve as a probe for both policies. Its permits must be coordinated so one policy does not hold a probe allowance while indefinitely waiting on another.

Reuse sampling duration, minimum throughput, failure threshold and break duration concepts from the existing breaker. Add configurable probe delivery limit, required successful probe count, probe operation timeout and bounded jitter for reopen scheduling. Validate finite thresholds, positive durations and consistent probe counts. Inject `TimeProvider` for scheduling and tests.

Default to one active half-open delivery probe and one successful probe to close. Successful probes required must not exceed the configured available probe count unless sequential probe rounds are explicitly supported.

Default classification proposals:

- Dependency: count final dispatch release/dead-letter outcomes caused by exceptions; exclude decode errors, shutdown cancellation and explicit non-error dispositions
- Expose an outcome predicate so applications can exclude poison-message/business failures and select dependency exceptions
- Observe once after the configured retry/redelivery middleware has produced the final dispatch result; do not count every in-process retry
- Retry middleware that republishes a message and reports success can hide the dependency failure. Document this and add explicit observation from that middleware if callers need the original failure counted; do not infer it from success
- Transport: classify recoverable connection/timeout errors using adapter knowledge; exclude empty polls, expected pause cancellation and terminal configuration/protocol failures
- Settlement errors/rejections remain separate structured evidence and are opt-in transport-policy failures, never dependency failures by default

These defaults are new pump-policy semantics, not a silent change to existing dispatch middleware behavior.

## Registration and core integration

Use the refactor's existing single source-factory registration and automatic execution selection. Enabling a circuit breaker does not require a transport to register a circuit-aware pump, alternative hosted service, polling runner or replacement factory.

`UsePumpCircuitBreaker` records validated configuration on the consumer descriptor (or descriptor-associated options). At lifetime creation, core constructs one coordinator for that descriptor and supplies it to the existing buffered/scoped polling admission path and callback control orchestration. Consumers without the feature use the refactor's normal admission behavior. Do not register one coordinator singleton keyed only by consumer type or create one per receive worker.

Register shared stateless policy helpers and the default clock centrally in `AddInterBus`, preserving an application-provided `TimeProvider`. Timers, probe accounting, observation generation and cancellation registrations are per-lifetime resources owned and disposed by core. Transport-specific retry/health classification and bounded callback-probe capability live on the existing adapters or their explicit capability interfaces, without a second parallel registration graph.

The core-owned polling loop is the enforcement point: every initial attempt and explicit retry reacquires admission and available capacity before invoking a transport iteration. Feature code extends that loop's admission coordinator rather than adding another loop or asking SQS/PostgreSQL adapters to wait on circuit state.

Registration validation covers conflicting middleware, invalid policy settings and unsupported callback probing. The application's transport setup remains unchanged; the consumer builder opt-in is the only additional application wiring.

## Admission and state machine

Implement one consumer-wide coordinator with enabled-policy state, circuit generations, scheduled transitions, outstanding acquisition/probe reservations and a terminal shutdown flag.

| State | Admission | Transition |
| --- | --- | --- |
| Closed | Existing capacity/concurrency limits | Classified failure threshold opens the policy |
| Open | No new acquisition | Break deadline makes probes eligible |
| Half-open | Bounded acquisition/delivery probes | Classified failure reopens; required successful probes close |
| Terminal | No acquisition or resume | Shutdown/fatal lifecycle failure wins permanently |

Core grants admission together with the required ownership capacity. Waiters must not retain unused capacity or probe permits while the circuit is open. On a transition, wake waiters to reevaluate instead of allowing a previously queued capacity waiter to bypass current admission.

Recheck state immediately before acquisition starts and record the operation as in flight. There is always a race with an operation already entering the SDK; define it as pre-open acquisition rather than claiming that pause retracts issued requests.

Tag acquisitions and delivery observations with their policy generation and whether they are recovery probes. Late pre-open outcomes may be logged, but cannot change a newer circuit generation or satisfy probe success. Record observation time at completion rather than handler start.

Opening stops subsequent acquisition, but does not cancel accepted processing. Drain all accepted work, including buffered deliveries that have not yet invoked handlers. Before admitting new delivery probes, wait for pre-open accepted work and acquisitions to finish, so old buffered work does not compete with the bounded recovery set. This can delay recovery behind slow handlers; preserve normal shutdown-deadline behavior instead of disposing their resources.

## Integration with the refactored source models

### Buffered polling

Extend the core reservation path to cap `ReceiveBatch.Capacity` by both ownership capacity and probe allowance. SQS must use that capacity as its request maximum; PostgreSQL lease claims one delivery per iteration.

- Transfer keeps the reservation's generation/probe metadata attached to the delivery
- Empty receives return unused delivery allowances; they demonstrate transport availability but cannot close a dependency circuit
- Empty-poll idle waits and retry delays occur after reservation release
- Empty probe polls use transport long polling or an explicit core delay to avoid busy loops
- A request started before opening can return a batch. Treat those deliveries as pre-open accepted work, renew them and drain them under normal ownership limits
- No new explicit retry can bypass admission; SDK retries inside an issued request remain an adapter limitation

### Scoped polling

Check admission before opening a connection/transaction or removing an in-memory message. An iteration receives a generation-tagged processing delegate and one probe allowance. Hold the transaction through delivery settlement and release it before returning the iteration result.

For transactional probes, publish final recovery evidence only once the iteration's required commit/rollback/cleanup boundary is known. A handler success followed by failed commit must not close a transport circuit. Never retry the whole processing iteration automatically after an ambiguous settlement.

### Callback intake

Core schedules pause/resume through the refactor's serialized control path. A delivery that trips the circuit publishes its outcome and continues settling/returning; it must not await pause completion from inside its own callback.

Basic pause/resume alone does not enforce a strict half-open delivery limit. Add an explicit capability for bounded resume, tentatively:

```csharp
public interface IProbeControlledCallbackSource : ICallbackSource
{
    Task ResumeProbesAsync(int deliveryLimit, CancellationToken operationToken);
}
```

The limit bounds total newly acquired probe deliveries for that resume generation, not merely concurrent callbacks. Each subsequent grant is explicit. A semaphore inside the processing callback is insufficient because it acquires broker-owned messages before admission.

Before shipping callback circuit support, investigate and test:

- Azure normal/session processor acquisition, prefetch, callback cancellation, renewal and whether a supported processor configuration can bound a probe generation
- RabbitMQ prefetch/credit, already queued callbacks, consumer cancellation and connection recovery restoring subscriptions while paused
- Empty-queue recovery and shutdown while a bounded subscription is active

If an adapter cannot guarantee bounded acquisition with the SDK, choose a documented adapter-specific probing mechanism or explicitly declare unsupported bounded probing. Do not advertise strict delivery probe limits based only on callback concurrency, silently release excess callbacks, or silently fall back to unlimited intake. Fail startup when requested guarantees are unsupported. This investigation is an implementation gate, not an assumed capability.

## Delivery outcome observation

Extend the refactor's internal observation path rather than infer health from completion of source tasks. Publish structured, generation-tagged observations for:

- Acquisition success/empty/failure and adapter retry classification
- Final dispatch result and decode failure
- Settlement success, rejection, exception or cancellation
- Delivery cleanup and scoped iteration completion

Keep dependency evidence separate from transport evidence. A transport failure must never cause a second settlement attempt. Observer failures cannot change delivery settlement or ownership; make the internal coordinator path nonthrowing and report diagnostic failures separately.

Use final required completion as the boundary for successful delivery probes. A classified dependency failure may request opening immediately after dispatch, while its delivery continues settlement. Settlement/cleanup failure or cancellation must not be misreported as successful recovery. An indeterminate probe either reopens with its reason or waits for the bounded operation timeout, releasing admission only through the coordinator.

## Existing middleware and compatibility

Keep `UseCircuitBreakerMiddleware` as the existing dispatch-only feature. Reuse policy implementation where practical, but do not share its open-circuit admission check with accepted pump deliveries.

Initially reject configuring both dispatch-only and pump circuit breakers on the same consumer, with an actionable configuration error. This avoids double counting and the old middleware rejecting drain work or probes. Document the migration to `UsePumpCircuitBreaker`; the pump feature observes outcomes without inserting a dispatch rejection path.

## Transport outage behavior

Recoverable receive errors feed the transport policy before core schedules another retry. Circuit delay and retry delay must combine without multiplying retry engines or bypassing admission. Retry exhaustion retains its terminal meaning unless a separately documented configuration changes it.

An empty receive is enough evidence to close an acquisition-only transport probe, but not a dependency probe. SDKs that automatically reconnect/retry need explicit adapter health observations and coordination with pause; logging an SDK error alone is insufficient.

Keep listener reconnection, visibility/lease renewal and settlement alive where they are needed for accepted work. Therefore “back off from transport” means suspend new intake, not guarantee zero broker/database operations during an outage. Do not cancel ownership maintenance merely because acquisition is paused.

Apply reopen jitter per consumer instance to reduce synchronized recovery. This reduces a thundering herd without promising cross-instance coordination.

## Lifecycle and diagnostics

Host shutdown/fatal failure sets terminal admission first, wakes paused/idle/capacity waiters, cancels timers and prevents every queued or delayed resume. Continue observing fatal source failures during a circuit pause and while callback control operations are pending.

Accepted work retains separate processing tokens and shutdown deadlines. Do not cancel processing simply to end an acquisition pause or timeout. If handlers ignore shutdown cancellation, retain their resources until they actually finish, as required by the existing lifetime contract.

Expose policy state, reason, generation, next eligible probe time, active probes and transition counts separately from `PumpState`. Include consumer identity and policy kind in logs. Emit transitions rather than logging a warning for every rejected acquisition waiter.

## Implementation sequence

1. Finish the pump refactor and its ownership/throughput validation. Resolve callback bounded-probe feasibility before promising transport parity.
2. Implement pure policy state and the admission coordinator using a controllable clock. Test generation handling, combined policies, capacity waits and shutdown precedence.
3. Connect buffered and scoped admission, acquisition observations and structured delivery outcomes. Add opt-in builder configuration and reject conflicting middleware.
4. Implement and verify adapter-specific callback pause and bounded probes. Declare unsupported guarantees explicitly where necessary.
5. Add diagnostics, configuration/migration documentation and integration coverage. Validate SQS steady-state throughput with the feature disabled and closed.

## Acceptance tests

Use fake time and explicit task barriers for core tests, plus actual transport integration tests for SDK behavior.

1. Multiple pumps share capacity and probe limits; opening prevents subsequent acquisition across all workers
2. Capacity waiters recheck state; half-open permits are not leaked during empty polls, delays, errors or shutdown
3. Late receive batches and callbacks retain ownership; accepted buffered work drains with renewal, bounded concurrency and ordering intact
4. Old-generation outcomes cannot close/reopen a newer circuit or satisfy its recovery requirement
5. Half-open success closes; classified failure reopens; empty dependency probes cannot close; probe timeout does not dispose active work
6. Dependency success with settlement failure is classified correctly; malformed/poison messages and shutdown cancellation follow the configured policy
7. PostgreSQL transactions survive through required completion; ambiguous commits never trigger processing replay or conflicting settlement
8. Callback controls cannot deadlock the triggering callback, cancel active processing, duplicate subscriptions or exceed advertised acquisition limits
9. SDK recovery cannot restore full intake while open; repeated transitions preserve session/FIFO ordering and acknowledgements
10. Shutdown/fatal failure wins during every wait/control phase, without delayed restart or leaked timers/registrations
11. A paused consumer does not stop healthy consumers; dependency and transport policies compose without deadlock
12. Existing dispatch-only behavior remains unchanged; conflicting configuration fails clearly
13. SQS prefetch and PostgreSQL lease buffering remain bounded and effective in closed state; steady-state throughput has no unexplained regression
14. Enabling the feature through the consumer builder alone wires one coordinator per lifetime using unchanged transport registrations; consumers without it retain normal admission

Record unavailable broker/database infrastructure and unresolved SDK guarantees explicitly. Do not mark callback support complete based solely on fake-source tests.
