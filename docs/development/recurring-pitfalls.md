# Recurring implementation pitfalls

These lessons come from the recent message pump implementation, refactor and follow-up fixes. They record prevention rules, not a list of outstanding bugs

For the current API, use the [message pump reference](../docs/reference/message-pumps.md). For execution and resource ownership, see [How message pumps work](../docs/explanation/message-pumps.md)

## Acquisition, ownership and pacing

- Acquisition and acknowledgement are separate outcomes. `PollResult.FoundWork` remains true for released, dead-lettered or unsuccessfully settled deliveries. Run a supplied `IdleWait` independently of `FoundWork`, after releasing acquisition resources and unused reservations, to avoid tight retry loops
- Reserve the shared consumer budget before receiving, bound requests by `ReceiveBatch.Capacity`, and dispose every acquired delivery that failed to transfer. A partial batch failure must preserve accepted deliveries while returning unused capacity
- Retry acquisition failures only. Once a scoped source invokes `process`, processing, settlement and surrounding scope-cleanup failures must not replay acquisition. Reset acquisition retry attempts after successful empty polls too
- Ownership and ordering last through settlement and cleanup. Do not release capacity, advance an ordering key, or dispose a transaction/SDK receiver while accepted work still uses it. Ambiguous settlement must not trigger a conflicting second settlement

Evidence: `4f55fd6` fixed transactional failure pacing; `90cbe2e` added focused coverage for partial transfer, deferred waits, retry reset and scoped cleanup. See `MessagePumpRuntimeTests` and transport adapter tests

## Fault supervision and shutdown

- Do not infer success from the task that wins `Task.WhenAny`. Intake completion or cancellation can race a published source failure; retain and report the original fault even when intake completes first
- Keep intake cancellation, accepted-processing deadlines and SDK control tokens separate. Expected startup cancellation proceeds through terminal pause and drain. Failed pause/resume controls can leave intake uncertain and must fault supervision
- Serialize callback controls, register callbacks once, and prevent resume after terminal shutdown. Stop new intake before draining accepted work, retaining renewal and settlement resources until that work finishes
- Avoid attaching per-instance continuations to a shared never-completing task. Racing local health against `SourceHealth.Never` can keep stopped adapters alive; use local health directly for that sentinel and verify collection when changing this path

Evidence: `495a722` retained faults during shutdown; `4f55fd6` corrected startup cancellation and callback retention. Regression coverage lives in `PumpLifetimeTests`, `MessagePumpRuntimeTests` and RabbitMQ `StartupCancellationTests`

## Test reliability and compatibility

- Framework upgrades can change async assertion behavior. Await NUnit 5 assertions and review new analyzer warnings rather than copying older test patterns
- Bound both the test body and teardown. Link fixture work to test cancellation and give shutdown a separate finite deadline
- Concurrent EF Core work needs separate database connections. Sharing an open SQLite connection between processor scopes and assertions caused the fixture to need rework
- Use signals to prove a source is actually blocked or ready before testing shutdown. Include races where failure and completion are already observable, not only sequential happy paths
- Core tests and fakes cannot establish SDK stop/restart or recovery guarantees. Exercise affected real container/emulator adapters, including Azure sessions and both PostgreSQL pump modes when applicable

Evidence: `962313c` isolated SQLite connections and bounded PostgreSQL fixture lifetimes; `495a722` cleaned test warnings; `3af0d1f` awaited NUnit 5 assertions. The [refactor validation report](../plans/message-pump-core-refactor-validation.md) records adapter coverage and benchmark limits

## Code organization and cleanup

- Separate top-level types into matching files, but keep private nested helpers with their containing class. Splitting every helper into a class-named directory led to a second cleanup pass
- Remove temporary profiling instrumentation after recording useful measurements. Keep implementation plans, scope, discussion and completion status in the Linear issue or a linked document, rather than repository planning or progress files
- Record the final changes, main tradeoffs and validation in the PR description. Keep lasting behavior in reference/explanation documentation, and reserve short ADRs in `docs/decisions/` for architectural choices future maintainers need to understand, such as a durable ID format or a deliberate limitation
- Update solution references when tooling changes: this repository now uses `InterBus.slnx`

Evidence: `0c3edbc` separated top-level types, then `868c9d9` restored nested helpers to their owners; `b255c60` removed temporary profiling and `f833dfb` removed completed investigation documents; `ab6fa9c` migrated the solution format
