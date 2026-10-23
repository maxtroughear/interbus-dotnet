# Message pump circuit breaker proposal

Status: implemented with local core, broker and database validation. See [validation results](message-pump-circuit-breaker-validation.md) and the [configuration reference](../docs/reference/message-pumps.md#intake-circuit-breakers). Live Azure validation remains outstanding. The proposal below records the original design and implementation baseline.

## Outcome and scope

Temporarily suspend one consumer's intake during a dependency or transport outage, then recover through controlled probes. Avoid repeated broker receives/releases while preserving accepted-delivery ownership, concurrency, ordering and settlement.

Circuit state belongs to one active consumer descriptor runtime. All of its buffered receive pumps, scoped workers and callbacks share it. Separate application instances have independent circuits; this feature does not globally pause a queue or coordinate distributed probes. Shared-dependency circuits across consumers are future work.

Keep one `PumpLifetime` initialized and supervised through open/half-open periods. Circuit state is separate from running/stopping/faulted lifecycle state. Terminal failures still fault the host; opening a circuit is not a source failure.

## Expected behavior

The feature changes when a consumer acquires more work. It preserves normal dispatch, settlement, ownership maintenance and shutdown behavior for work already accepted.

| Situation | Expected behavior |
| --- | --- |
| Feature disabled | Existing intake, concurrency, prefetch and retry behavior continues unchanged |
| Enabled and closed | Normal throughput and ownership limits apply; final outcomes feed the configured policies |
| Classified failures reach a policy threshold | That policy opens and all acquisition workers for this consumer stop taking new work; other consumers continue |
| Work was accepted before opening | Buffered deliveries, callbacks and scoped processing continue through settlement and cleanup; renewal stays active |
| An acquisition was already in flight | Its returned deliveries remain owned, are tagged as pre-open work and drain normally |
| Break duration expires | After pre-open work drains, admit the bounded recovery set; do not resume unrestricted intake |
| A delivery probe succeeds | Count recovery only after its required dispatch, settlement, cleanup and enclosing scope boundaries succeed in the current generation |
| A probe has a classified failure | Reopen the affected policy and schedule another recovery opportunity; finish handling accepted work normally |
| A probe has an uncertain outcome or times out | Do not report recovery success; resolve it through the bounded timeout/reopen policy without canceling accepted processing or disposing resources still in use |
| A transport probe finds an empty queue | It can establish acquisition health, but supplies no dependency recovery evidence and cannot override another open policy |
| Every enabled policy has recovered | Restore ordinary intake using the same source and lifetime; adapters may recreate SDK processors/subscriptions as required |
| Shutdown or fatal source/control failure occurs | Terminal lifecycle handling wins, prevents future resumes and retains resources until accepted work finishes |

With the default half-open limit, only one new delivery probe is permitted across all receive pumps or scoped workers. Increasing `PumpCount` or `ConcurrencyCount` must not multiply that allowance. Callback transports must enforce the same advertised acquisition bound through an explicit supported capability.

## Alignment with the implemented core

The source interfaces and single-factory registration already fit this design. Add the remaining circuit behavior to the existing core rather than building replacement transport loops.

| Existing implementation | Work required for this feature |
| --- | --- |
| `ConsumerRuntime.PollAsync` owns polling and retry scheduling | Add coordinated admission before capacity/resource acquisition and recheck it immediately before invoking the source; there is no admission coordinator yet |
| `ObserveAcquisition` reports only `PollResult` or an exception | Replace or extend it with structured classification, operation identity, generation and probe attribution |
| `ReceiveBatch`, `DeliverySink.Entry` and scoped processing own work | Carry generation/probe metadata through transfer and track accepted work until its required completion boundary |
| `DeliveryProcessor` preserves settlement recovery behavior | Publish explicit dispatch, settlement and cleanup outcomes; a normally completed task is not proof of successful settlement |
| `CallbackIntake` serializes ordinary start/pause/resume | Route bounded probe grants through this same control gate, with explicit grant accounting and terminal-stop precedence |
| Core retry delays use `Task.Delay` directly | Use the injected `TimeProvider` for core retry scheduling, circuit transitions and probe timeouts so their interaction is testable |

Ordinary Azure normal/session pause/resume and RabbitMQ drain/recovery have been exercised with the actual SDKs and container infrastructure. An isolated Azure SDK handoff prototype also established the normal/session probe mechanisms described below, including the need to dispose a stopped session processor. Full circuit integration, RabbitMQ probe integration and strict acquisition-bound coverage remain implementation gates.

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

`UsePumpCircuitBreaker` records validated configuration on the consumer descriptor (or descriptor-associated options). At lifetime creation, core constructs one coordinator for that descriptor and supplies it to the buffered/scoped polling execution paths and existing callback control orchestration. Consumers without the feature retain the refactor's normal capacity/concurrency behavior. Do not register one coordinator singleton keyed only by consumer type or create one per receive worker.

Register shared stateless policy helpers and the default clock centrally in `AddInterBus`, preserving an application-provided `TimeProvider`. Timers, probe accounting, observation generation and cancellation registrations are per-lifetime resources owned and disposed by core. Transport-specific retry/health classification and bounded callback-probe capability live on the existing adapters or their explicit capability interfaces, without a second parallel registration graph.

The core-owned polling loop is the enforcement point: every initial attempt and explicit retry reacquires admission and available capacity before invoking a transport iteration. Add the coordinator to that loop and its reservation path rather than adding another loop or asking SQS/PostgreSQL adapters to wait on circuit state.

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

Basic pause/resume alone does not enforce a strict half-open delivery limit. RabbitMQ and Azure Service Bus must support bounded half-open delivery probes. Keep their callback subscriptions/processors as the closed-state fast path, and use explicit single-message receives while half-open. This is a required adapter capability, not an optional fallback to unrestricted resume.

Add an explicit capability for one bounded acquisition, tentatively:

```csharp
public interface IProbeControlledCallbackSource : ICallbackSource
{
    Task<PollResult> ReceiveProbeAsync(CancellationToken processingToken, CancellationToken operationToken);
}
```

One invocation may acquire at most one new delivery, with prefetch disabled. It feeds the delivery through the same processing delegate, dispatch, settlement and structured outcome path installed for normal callbacks, and retains its resources through required cleanup. The return value reports acquisition only; it is not delivery recovery evidence. Core attaches a generation-tagged grant before acquisition and explicitly authorizes every subsequent call. Multiple configured probes share the consumer-wide allowance rather than multiplying it per source or callback.

Extend `CallbackIntake` to serialize probe acquisition/control transitions with start, pause, ordinary resume and terminal stop. Its current paused/running flag is insufficient for tracking probe grants. Keep ordinary intake paused throughout probing; neither another grant nor SDK recovery may restore unlimited intake. Do not bypass the wrapper by invoking an adapter capability directly. Separate acquisition-operation cancellation from accepted processing ownership, and retain terminal-stop precedence throughout probe processing and cleanup. Preserve the existing fatal treatment of failed or canceled SDK controls whose intake state is uncertain; ordinary circuit opening is a separate, nonfatal transition.

A callback concurrency limit, semaphore or prefetch of one does not bound total deliveries over time. Do not silently acquire and release excess callbacks. Built-in RabbitMQ and Azure support must implement the mechanisms below before shipping; other adapters without a verified bounded capability must fail startup when these guarantees are requested.

#### RabbitMQ

Use the existing callback consumer in closed state. On opening, cancel the consumer through serialized controls and drain callbacks and deliveries already accepted, preserving their acknowledgements and channel ownership. Track in-flight cancellation deliveries as pre-open work. Connection recovery must not recreate the ordinary subscription while open or half-open.

For each half-open grant, call `BasicGetAsync` with manual acknowledgement. It returns at most one message; an empty result supplies acquisition evidence only. Wrap a returned message in the normal delivery representation, then dispatch and acknowledge/nack it through the shared pipeline. Keep its channel usable until settlement and cleanup complete. Restore the callback subscription only after all enabled policies recover. This uses polling only for bounded recovery; normal throughput continues through callback consumption. See the [RabbitMQ .NET API guide](https://www.rabbitmq.com/client-libraries/dotnet-api-guide#basic-get).

Prefer this explicit receive over a temporary callback subscription with prefetch one: an acknowledgement can replenish credit and allow another delivery before cancellation completes. A callback-only alternative would need cancellation before settlement and separate cancellation/drain controls to avoid awaiting the callback that requested pause; it is not the required initial implementation.

#### Azure Service Bus

Use `ServiceBusProcessor` and `ServiceBusSessionProcessor` in closed state. On opening, stop ordinary processing and await accepted callbacks before acquiring probes. Do not await processor stop from the callback whose outcome triggered opening.

For normal queues, retain the stopped processor and create a standalone receiver with `ReceiveMode = PeekLock` and `PrefetchCount = 0`. Each grant invokes `ReceiveMessageAsync` once with a bounded wait, then uses the shared delivery pipeline. Dispose the probe receiver after settlement and cleanup. Restart the ordinary processor only after recovery. An empty receive returns `null`; it supplies no dependency recovery evidence. See [single-message receive](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusreceiver.receivemessageasync?view=azure-dotnet).

Session queues require an explicit ownership handoff:

1. Stop and drain the session processor, then dispose it to release its session locks. `StopProcessingAsync` alone is insufficient for this handoff
2. For an authorized grant, use `AcceptNextSessionAsync` with `PeekLock` and `PrefetchCount = 0`, then invoke `ReceiveMessageAsync` once
3. Process and settle that message while retaining the session receiver and lock. The probe dispatches one message, but ownership covers the whole session
4. Dispose the probe session receiver after required processing, settlement and cleanup, before ordinary intake resumes
5. Once every enabled policy recovers, create a fresh session processor with the original concurrency, ordering and renewal settings, attach callbacks once and start it through the serialized control path

Retain the existing source and `PumpLifetime` throughout this processor replacement. It must not repeat topology initialization, create a second runtime or duplicate callback registrations. Repeated probes leave ordinary intake stopped and retain no session receiver after its work completes.

`AcceptNextSessionAsync` can throw `ServiceTimeout` when no unlocked session contains active messages. Treat this as potentially no available probe work, not automatic outage evidence or dependency recovery. Do not blanket-exclude unrelated receive/communication timeouts. Bound total acquisition time, including SDK retries, and distinguish expected no-work responses, operation cancellation and actual transport failures. See [session acceptance](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusclient.acceptnextsessionasync?view=azure-dotnet).

Standalone probe receivers do not inherit the processor's automatic lock renewal. The adapter must maintain message locks for normal probes and session locks for FIFO probes until required settlement/cleanup completes, including while a circuit reopens or a probe deadline expires. Session renewal covers the messages in that session. Lock loss or failed/indeterminate settlement must prevent a fully successful delivery probe. See [message lock renewal](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebusreceiver.renewmessagelockasync?view=azure-dotnet) and [session lock renewal](https://learn.microsoft.com/en-us/dotnet/api/azure.messaging.servicebus.servicebussessionreceiver.renewsessionlockasync?view=azure-dotnet).

An isolated prototype against the Service Bus emulator with `Azure.Messaging.ServiceBus` 7.21.0 validated the handoff. A stopped normal processor allowed one direct receive, explicit lock renewal, settlement and callback restart. A stopped session processor retained the target session: `AcceptNextSessionAsync` timed out despite queued work, and restarting that processor consumed it. Disposing the drained session processor and recreating it after the probe passed. Both final cases completed one probe and dispatched the two remaining messages once; session order was preserved. These checks exercised SDK handoff rather than full circuit admission or probe outcome integration. Live Azure validation remains outstanding.

## Delivery outcome observation

Publish internal typed observations, not inferred success from task completion or parsed logs. The current `DeliveryProcessor` logs and absorbs settlement exceptions and rejections to preserve transport recovery. `ProcessAsync` and even a scoped iteration can therefore return normally after a failed settlement. `PollResult.FoundWork` describes acquisition only and cannot establish successful delivery or recovery.

Observe the stages independently:

| Stage | Required evidence |
| --- | --- |
| Acquisition | Success, empty response or exception, plus adapter error classification and the evaluated retry decision |
| Decode | Successful decoding or a decode error, kept distinct from handler/dependency failure |
| Dispatch | The final `DispatchResult` after configured middleware, including explicit dispositions and expected processing cancellation |
| Settlement | Acknowledgement/release/dead-letter result: accepted, rejected, exception, cancellation or an indeterminate broker/commit outcome |
| Delivery cleanup | Disposal completion or failure, regardless of preceding dispatch/settlement result |
| Scoped iteration completion | Completion of required surrounding transaction/connection cleanup after the processing delegate returns |

Every observation carries its acquisition/delivery correlation identity, stage, outcome, relevant result or exception, cancellation reason, completion time, circuit generation and probe identity where applicable. Define the internal representations during implementation; these do not require a new application-facing delivery API. Attach this metadata when admission is granted and preserve it across buffered handoff, scoped processing and callbacks rather than reading the current generation when processing finishes.

Use staged observations and a final completion summary. A classified dependency failure may request opening immediately after final dispatch, while its delivery continues settlement. Successful delivery probes require the configured successful completion boundaries; do not publish success at handler return and attempt to retract it after cleanup fails. Each stage contributes at most once to its configured policy, and each probe contributes at most one final recovery result. Observer/predicate failures must not escape into processing, alter ownership, change settlement behavior or trigger another settlement attempt; report diagnostic failures separately.

Expected classification examples:

| Observed delivery | Policy interpretation |
| --- | --- |
| Dependency timeout produces release; release succeeds | Count the classified dependency failure; successful release does not make this a successful delivery probe |
| Dispatch succeeds; acknowledgement fails or its outcome is uncertain | Retain dispatch success separately; treat settlement as transport evidence only when enabled, and do not report a fully successful delivery probe |
| Decode fails | Apply the configured malformed-message policy without treating the decode error as a dependency failure by default |
| Acquisition succeeds with no message | Supply transport acquisition evidence only; no dependency delivery probe succeeded |
| Shutdown cancels processing | Exclude expected cancellation from failure sampling and do not count recovery success |
| Old-generation delivery finishes successfully | Retain diagnostics but do not close a newer generation or satisfy its probe count |

For example, a PostgreSQL handler can succeed, `CommitAsync` can then throw, `DeliveryProcessor` can log the settlement failure, and transaction disposal can finish without another exception. The iteration then returns `FoundWork = true`. The observations must retain the failed/indeterminate commit and prevent that normal return from closing a delivery-based recovery probe. Waiting for scoped iteration completion is necessary but is not sufficient without the explicit settlement outcome.

Keep dependency evidence separate from transport evidence. A settlement failure does not become a dependency failure merely because it prevents successful probe completion. An indeterminate probe either reopens with its reason or waits for the bounded operation timeout, releasing admission only through the coordinator. Resource ownership continues until the accepted work and its enclosing scope actually finish.

## Existing middleware and compatibility

Keep `UseCircuitBreakerMiddleware` as the existing dispatch-only feature. Reuse policy implementation where practical, but do not share its open-circuit admission check with accepted pump deliveries.

Initially reject configuring both dispatch-only and pump circuit breakers on the same consumer, with an actionable configuration error. This avoids double counting and the old middleware rejecting drain work or probes. Document the migration to `UsePumpCircuitBreaker`; the pump feature observes outcomes without inserting a dispatch rejection path.

## Transport outage behavior

Recoverable receive errors feed the transport policy before core schedules another retry. Circuit delay and retry delay must combine without multiplying retry engines or bypassing admission. Retry exhaustion retains its terminal meaning unless a separately documented configuration changes it.

Integrate with `IPollingRetryPolicy.GetRetryDelay(error, attempt)`: evaluate a retry decision once and publish its classification alongside that decision. A null delay currently means terminal handling, which can result from either an ineligible error or an exhausted retry budget; it is not enough to distinguish those causes for policy sampling. Extend the decision/classification representation where needed without introducing a second retry engine. Use the shared core `TimeProvider` for both retry and circuit scheduling; SDK-internal retries and ownership-maintenance timers retain their adapter contracts.

An empty receive is enough evidence to close an acquisition-only transport probe, but not a dependency probe. SDKs that automatically reconnect/retry need explicit adapter health observations and coordination with pause; logging an SDK error alone is insufficient.

Keep listener reconnection, visibility/lease renewal and settlement alive where they are needed for accepted work. Therefore “back off from transport” means suspend new intake, not guarantee zero broker/database operations during an outage. Do not cancel ownership maintenance merely because acquisition is paused.

Apply reopen jitter per consumer instance to reduce synchronized recovery. This reduces a thundering herd without promising cross-instance coordination.

## Lifecycle and diagnostics

Host shutdown/fatal failure sets terminal admission first, wakes paused/idle/capacity waiters, cancels timers and prevents every queued or delayed resume. Continue observing fatal source failures during a circuit pause and while callback control operations are pending.

Accepted work retains separate processing tokens and shutdown deadlines. Do not cancel processing simply to end an acquisition pause or timeout. If handlers ignore shutdown cancellation, retain their resources until they actually finish, as required by the existing lifetime contract.

Expose policy state, reason, generation, next eligible probe time, active probes and transition counts separately from `PumpState`. Include consumer identity and policy kind in logs. Emit transitions rather than logging a warning for every rejected acquisition waiter.

## Implementation sequence

1. Use the completed pump refactor and its validation as the baseline. Implement the required RabbitMQ single-get and Azure normal/session receiver mechanisms above; preserve the Azure session ownership handoff established by the prototype.
2. Define structured stage observations and probe completion accounting, preserving existing settlement semantics. Implement pure policy state and the admission coordinator using a controllable clock.
3. Connect `PollAsync`, buffered reservations and scoped admission to the coordinator. Carry generation/probe metadata through ownership, add outcome publication, integrate retry decisions and add opt-in builder configuration with conflicting-middleware validation.
4. Extend the existing serialized callback controls with the required RabbitMQ and Azure bounded probes, session processor replacement and probe lock renewal. Verify actual acquisition bounds and recovery behavior; reject unsupported capabilities on other adapters explicitly.
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
15. Normally completed processing/scoped tasks with rejected, failed or indeterminate settlement cannot falsely close a delivery probe; dispatch and settlement evidence remain separately classified
16. A dispatch-stage failure can request opening before settlement completes without deadlocking its callback; observers that throw cannot alter delivery handling or cause duplicate observations/settlement
17. Acquisition classification distinguishes terminal errors from exhausted retry budgets, and fake time controls combined retry/circuit delays without duplicate policy evaluation
18. Success followed by delivery/scope cleanup failure is not counted as successful probe completion; repeated bounded callback grants do not restore unlimited intake

19. RabbitMQ manual-ack single-get probes acquire at most the granted delivery count even with backlog, high consumer concurrency and repeated grants; ordinary subscriptions remain canceled across connection recovery until authorized resume
20. Azure normal probes disable prefetch, acquire one message per grant, settle through the shared pipeline and restart the retained processor without duplicate callbacks
21. Azure session probes release old processor ownership before acceptance, dispatch one message while retaining the whole session lock, dispose the probe receiver before resume and recreate the ordinary session processor without duplicate registrations or FIFO violations
22. Long-running Azure probes renew message/session locks through settlement and cleanup; renewal failure, lock loss and ambiguous settlement cannot falsely close a probe or trigger conflicting settlement
23. Empty Azure receives and no-available-session timeouts cannot close dependency circuits or be mistaken automatically for outages; total acquisition waits and SDK retries remain bounded
24. Opening, reopening, shutdown and fatal failure during Azure session handoff, probe renewal or processor recreation retain accepted work and prevent unauthorized processor restart

Record unavailable broker/database infrastructure and unresolved SDK guarantees explicitly. Do not mark callback support complete based solely on fake-source tests.
