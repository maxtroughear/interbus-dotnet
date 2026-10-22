# Message pump core refactor validation

Validated on 2026-10-21 using .NET SDK 10.0.402 on macOS arm64.

## Regression results

| Suite | Passed | Skipped |
| --- | ---: | ---: |
| Core runtime, lifetime and in-memory | 193 | 0 |
| Amazon SQS | 40 | 1 |
| PostgreSQL | 64 | 0 |
| Azure Service Bus | 26 | 0 |
| RabbitMQ | 11 | 0 |
| Total | 334 | 1 |

The solution builds successfully. Three existing nullable-reference warnings remain in EventBridgeDeliveryTests and PostgreSQL RequestReplyTests. `git diff --check` passes.

Container integration suites ran against LocalStack, PostgreSQL 17 and 18 in both pump modes, the Azure Service Bus emulator and RabbitMQ. The EventBridge scheduler test requires real AWS credentials and was skipped. Azure's 26 passing tests comprise the full 24-test suite plus two additional emulator tests for reversible intake on normal and session processors.

New coverage includes unused batch capacity before deferred waits, partial ownership transfer followed by acquisition failure, expired batch and over-transfer rejection, retry-count reset after empty polls, scoped cleanup ownership and retry exclusion, prompt idle-wait failure reporting, execution-model rejection before initialization, serialized callback shutdown, repeated pause/resume, RabbitMQ recovery while paused, PostgreSQL prefetch bounds across pumps and sequential in-memory redelivery ordering.

## Callback SDK behavior

The pinned Azure.Messaging.ServiceBus 7.21.0 package's XML API documentation confirms that normal and session processor stop operations retain receivers, await active handlers and support subsequent startup. Canceling a stop can leave intake running, so a failed control faults core supervision and triggers terminal pause with an independent token. See the [normal processor stop contract](https://learn.microsoft.com/dotnet/api/azure.messaging.servicebus.servicebusprocessor.stopprocessingasync) and [session processor stop contract](https://learn.microsoft.com/dotnet/api/azure.messaging.servicebus.servicebussessionprocessor.stopprocessingasync).

Normal and session adapter tests cover callback cancellation during pause and settlement after lock loss. Emulator tests exercise three pause/resume cycles, including duplicate resume requests, with the actual SDK.

RabbitMQ.Client 7.2.2 uses subscription cancellation and consumer unregistration before callback drain, retaining channels for settlement. Existing drain/deadline tests and new pause/resume tests ran against RabbitMQ, including connection recovery while paused. The [RabbitMQ .NET client guide](https://www.rabbitmq.com/client-libraries/dotnet-api-guide) describes concurrent callback dispatch and automatic recovery; intake control remains outside callbacks.

## SQS before/after comparison

Baseline: commit `b2afdaf0f3acfb75ff4973c15e746437cb4f24a1`, exported to an isolated temporary checkout. The same throughput fixture and instrumented fake SQS client ran against baseline and refactored code.

Each sample processes 512 deliveries with concurrency 8, three receive pumps, maximum batch size 10, 2 ms simulated receive latency and 5 ms asynchronous handler work. Prefetch is either zero or 32. Results below are medians of three samples per configuration. Utilization measures time handlers occupy processing slots, rather than CPU utilization. Receive counts include requests canceled when the queue is exhausted.

| Prefetch | Elapsed ms before / after | Receive calls before / after | Peak acquired deliveries before / after | Handler utilization before / after |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 512.9 / 511.1 | 451 / 441 | 8 / 8 | 64.2% / 64.1% |
| 32 | 356.2 / 357.3 | 254 / 243 | 40 / 40 | 97.2% / 97.4% |

Median elapsed time changes by -0.35% without prefetch and +0.31% with prefetch. Sample ranges overlap; the differences are within observed scheduling variation. Receive volume does not increase, utilization remains comparable and acquired ownership stays within the shared budget. This harness shows no meaningful regression; it is not a live AWS throughput benchmark.

Raw elapsed times in milliseconds:

| Prefetch | Baseline samples | Refactored samples |
| ---: | --- | --- |
| 0 | 515.4, 502.6, 512.9 | 511.1, 510.3, 512.3 |
| 32 | 347.0, 356.2, 356.6 | 347.8, 358.8, 357.3 |

Reproduce with the `ReceiveThroughput_WithFixedLatency` filter in `InterBus.AmazonSqs.Tests`. Set `INTERBUS_PUMP_BENCHMARK_OUTPUT` to a writable output file to capture measurements. Core ownership tests separately cover reservations, buffered deliveries and active processing; the benchmark's ownership counter measures acquired deliveries.

## PR review follow-up

Validated on 2026-10-22 after addressing the three review findings on PR #293:

- Transactional acquisition still reports `FoundWork = true` after release or unsuccessful settlement, while a separate deferred wait preserves notification/fallback pacing after transaction and connection disposal. Core invokes the deferred wait independently of acquisition success
- Callback sources using default health no longer race the shared never-completing task against local health, allowing stopped adapters and lifetimes to be collected
- Expected cancellation during callback startup proceeds through terminal pause and drain without publishing a fatal failure. RabbitMQ distinguishes startup subscription cancellation from resume failure; canceled pause/resume controls remain fatal

| Suite | Passed |
| --- | ---: |
| Full core runtime, lifetime and in-memory suite | 197 |
| PostgreSQL transactional pacing, settlement faults, context and notifications (17 and 18) | 18 |
| Full RabbitMQ suite | 13 |
| Full Azure Service Bus suite | 26 |
| Total | 254 |

The strengthened blocked-start readiness check and other lifetime tests also passed in a separate 22-test run. New coverage verifies deferred pacing after cleanup, release acquisition reporting and connection disposal, collection after clean callback shutdown, blocked-start shutdown, fatal pause cancellation, and RabbitMQ startup versus resume subscription cancellation. Integration checks used local containers/emulators. The solution build and `git diff --check` passed; existing nullable-reference warnings remain.
