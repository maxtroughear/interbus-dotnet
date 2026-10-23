# Agent Guidelines

- Keep communication concise and clear
- Do not overuse punctuation; not every comment needs to end with a period
- In code, avoid comments that restate what the code does; comment only to explain unusual behavior or complex reasoning
- Public API interface documentation should be complete; internal-only documentation can be lighter
- Prefer concise explanations and minimal planning over wordy detail

## Code organization and style

- Give top-level types their own matching files; avoid catch-all files such as `Consumers.cs` or `TestMessages.cs`
- Keep private helper types nested in their containing class and in the same file; do not introduce partial classes or class-named directories just to split nested helpers
- Follow the surrounding project's conventions rather than applying unrelated formatting changes
- When changing a public contract, document ownership, cancellation, failure behavior and concurrency guarantees as applicable, and update the relevant reference documentation

## Implementation and verification

- Before changing message pumps or transport adapters, read [recurring implementation pitfalls](docs/development/recurring-pitfalls.md) and the linked adapter contracts
- Preserve transport-specific settlement, ordering and shutdown behavior when moving work into shared abstractions
- Validate affected core behavior and transport adapters; a passing core suite alone does not establish transport parity
- Use `InterBus.slnx` for solution commands and tooling; the old `.sln` has been removed
- Check the diff for whitespace errors and new compiler/analyzer warnings; report existing warnings separately
- Remove temporary profiling hooks and obsolete implementation notes once their useful results are captured in the appropriate location below

## Documentation placement

- Keep implementation plans, scope, discussion and completion status in the Linear issue or a document linked from it; do not add repository planning or progress files
- Use the PR description to record what changed, the main tradeoffs and validation, including any checks that could not be run
- Keep short architecture decision records in `docs/decisions/` only for choices future maintainers need to understand, such as a durable ID format or a deliberate limitation. Record the decision, rationale and consequences; do not use ADRs as implementation plans or progress logs
- Keep lasting user-facing behavior and API contracts in the relevant reference or explanation documentation

