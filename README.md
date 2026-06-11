# interbus-dotnet

Interoperable asynchronous messaging framework for .NET

## Architecture at a glance

InterBus separates concerns into three layers:

- `InterBus` core: contracts, message dispatch pipeline, middleware, routing conventions, scheduling abstractions, and saga abstractions
- Transport packages: broker-specific delivery implementations behind shared transport contracts
- Integration packages: persistence, scheduling providers, and compile-time registration support

At runtime, applications publish or send messages through `IPublisher`; transport pumps receive raw payloads and dispatch to typed consumers (`IConsumer<T>`) inside DI scopes. Optional middleware and saga infrastructure add retries, stateful workflows, scheduling, and durable delivery patterns.

## Current support in this repository

### Transports

- In-memory (built into `InterBus` via the `InterBus.InMemory` namespace)
- Amazon SNS/SQS (`InterBus.AmazonSqs`)
- Azure Service Bus (`InterBus.AzureServiceBus`)
- RabbitMQ (`InterBus.RabbitMQ`)
- PostgreSQL-backed transport (`InterBus.PostgreSQL`)

### Integrations

- EF Core integration (`InterBus.EntityFrameworkCore`) for outbox and saga persistence
- DynamoDB saga persistence (`InterBus.DynamoDB`)
- TickerQ scheduler provider (`InterBus.TickerQ`)
- Source generator support (`InterBus.SourceGenerator`) for compile-time registration and reflection-free message dispatch

### Feature areas

- Message publishing and sending
- Consumer middleware pipeline
- Retry middleware
- Saga state orchestration (including completion and deletion semantics)
- Message scheduling abstractions and providers
- Outbox integration (EF Core)

## More details

- `docs/docs/index.md`
- `docs/docs/tutorials/getting-started.md`
- `docs/docs/explanation/architecture.md`
