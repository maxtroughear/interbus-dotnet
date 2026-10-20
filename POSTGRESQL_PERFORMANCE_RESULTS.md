# PostgreSQL load investigation

The lease load consumer now uses four receive pumps and 40 shared processing slots. Transactional mode retains one managed source with 40 transaction-owning workers. This changes the load-test configuration, not the default for every PostgreSQL consumer.

The workload and assertions remain unchanged: 200,000 messages per case, 20 publishers, 40 processing slots, two minutes to drain after publication, and matching counts and message IDs. Cancellation stops publishing rather than reporting unfinished batches as fully published. Progress logging counts successful publishes and actual receives; the consumption rate now uses the actual received count.

## Comparison with other transport tests

These are configured workloads, not normalized throughput benchmarks:

| Transport | Messages per case | Publishers | Processing concurrency | Drain wait after publishing |
| --- | ---: | ---: | ---: | --- |
| PostgreSQL | 200,000 | 20 | 40 | 120 seconds |
| SQS | 10,000 | 10 | 20 | 120 seconds |
| RabbitMQ | 10,000 | 100 | 50 | 120 seconds |
| Azure Service Bus | 200 | 2 | 10 | 120 seconds |

PostgreSQL processes 20 times the SQS/RabbitMQ volume and 1,000 times the Azure volume per case, across four mode/version combinations. Other transports passing their tests does not establish that PostgreSQL should finish this workload within the same deadline. All-message correctness remains required; the fixed deadline is also a hardware-dependent throughput requirement.

## Local measurements

All measurements below use the full 200,000-message workload and real PostgreSQL containers on the same macOS ARM64 host, sequentially. They are individual diagnostic runs, not statistically established performance guarantees or GitHub runner measurements. Baseline means `f14bf59`, before the shared-runtime implementation, with the same progress diagnostics added in a temporary checkout.

| PostgreSQL 18 configuration | Publish complete | All delivered | Result |
| --- | ---: | ---: | --- |
| Lease, one receive pump, current runtime | 9.1 seconds | 81.3 seconds | Pass |
| Lease, four receive pumps, current runtime | 10.2 seconds | 35.4 seconds | Pass |
| Transactional, current runtime | 18.7 seconds | 20.9 seconds | Pass |
| Transactional, baseline runtime | 19.1 seconds | 20.8 seconds | Pass |
| Transactional, current, smaller CPU budget | 72.0 seconds | 95.0 seconds | Pass |
| Transactional, baseline, smaller CPU budget | 80.4 seconds | 95.3 seconds | Pass |

Both PostgreSQL 17 cases also passed: lease with four pumps published in 10.3 seconds and delivered everything in 34.9 seconds; transactional published in 18.4 seconds and delivered everything in 20.6 seconds.

All 55 PostgreSQL parity tests passed again after these changes. The tests use the current production registrations; full-volume runs also retain the same count and message-ID assertions.

The smaller CPU budget uses `DOTNET_PROCESSOR_COUNT=2` for the test process and a Docker two-CPU quota applied to the isolated database near startup. This is a sensitivity check, not an exact reproduction of GitHub's Linux runner. The baseline database used approximately 191% CPU of its 200% quota during the drain. Samples showed short-lived PostgreSQL lock-manager/object waits and short transactions, with roughly 60 pooled sessions rather than exhaustion of the default 100-connection pool.

Four receive pumps reduced the lease run's time to deliver all messages by approximately 2.3 times compared with one pump. The transactional comparisons did not reproduce a refactor slowdown locally, either with the normal environment or smaller CPU budget. They show substantial database CPU sensitivity, but do not establish the cause of the GitHub publishing stalls. No production transactional behavior has been changed on this evidence.

## Runner investigation

The previous runner run failed two cases after their drain deadline and cancelled two cases while publishing after ten minutes. The new load test records progress every fifteen seconds, processor/thread-pool counters, and active database sessions grouped by state, wait event, query and oldest transaction age using a separate observer pool. Diagnostics are both streamed during direct runs and retained in the test adapter's captured output. This allows the next runner result to distinguish a publish stall, receive backlog, long transaction, database lock wait or transport pool pressure without diagnostics waiting behind the transport pool.

The CI workflow changes that separate full load tests from parity tests remain pending locally because the authenticated GitHub credential lacks the `workflow` scope. The four-pump configuration and load diagnostics are pushed independently; the runner therefore still executes the full workload in its PostgreSQL job.
