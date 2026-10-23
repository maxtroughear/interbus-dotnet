# Test guidelines

- Match the test framework used by the project: core tests use xUnit, while many transport suites use NUnit
- Await async assertions, including NUnit 5 `Assert.ThrowsAsync`; an unawaited assertion can leave failures unobserved
- Pass the framework's test cancellation token to operations and waits unless the test intentionally models a separate lifecycle token
- Bound readiness waits, background loops and teardown. Teardown needs a fresh deadline when the test token is already canceled
- Use explicit signals to coordinate concurrency tests and establish the state under test before asserting; avoid relying on elapsed delays to prove readiness or shutdown
- Give concurrently used EF Core scopes independent connections. Do not share one open SQLite connection between background processing and assertions; isolate each test's database and clean it up
- For pump changes, cover failure and cancellation around acquisition, handoff, settlement and cleanup, including partial transfers and shutdown races where relevant
- Preserve the load fixture's workload and configuration when comparing throughput, distinguish fake-client measurements from live-service results, and keep diagnostic waits bounded
- Run affected container/emulator suites when available and state any skipped dependencies. Keep real AWS CI jobs restricted to pushes to `main`, as configured in `.github/workflows/build.yml`

See [recurring implementation pitfalls](../docs/development/recurring-pitfalls.md) for the fixes behind these rules
