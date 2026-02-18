# InterBus

Interoperable asynchronous messaging-based application framework for .NET

Key architectural components

1. Publisher
    1. To an exchange/router/topic
        1. Abstraction name "Topic"

    2. Topic per message type or ~Topic per publisher~
        1. Could it support both with complex routing? Keep in mind, dumb pipes and smart endpoints


2. Sender?
    1. Directly to a consumer endpoint - does this need to be supported? If so, should think about how it looks fairly early in the overall architecture

3. Consumer
    1. Has an endpoint (queue)

4. Message pump (service activator)
    1. Connects a transport to consumer(s)
    2. Should we have multiple message pumps per consumer?
        1. Concurrent polling (multiple channels for RabbitMQ) and dispatching one-by-one
        2. Prefetching many messages and dispatching concurrently
        3. Could this be transport specific?
        4. 


Key features and principles

1. No reflection - source generators and generic code
    1. Inspired by TickerQ [github.com/Arcenox-co/TickerQ/blob/main/src/TickerQ.SourceGenerator/TickerQIncrementalSourceGenerator.cs](https://github.com/Arcenox-co/TickerQ/blob/main/src/TickerQ.SourceGenerator/TickerQIncrementalSourceGenerator.cs) 
    2. [github.com/dotnet/roslyn/blob/main/docs/features/incremental-generators.cookbook.md](https://github.com/dotnet/roslyn/blob/main/docs/features/incremental-generators.cookbook.md) 

2. Only source generators for triggering consumer code
    1. [www.enterpriseintegrationpatterns.com/patterns/messaging/MessagingAdapter.html](https://www.enterpriseintegrationpatterns.com/patterns/messaging/MessagingAdapter.html) 

3. Microsoft DI first
4. Strive to support Native AOT but will probably bound by dependencies
    1. EF core - experimental

5. AWS SDK
6. Rabbit MQ SDK/client
7. Channels for an in memory implementation?
    1. [learn.microsoft.com/en-us/dotnet/core/extensions/channels](https://learn.microsoft.com/en-us/dotnet/core/extensions/channels) 

8. Explicit registration of consumers
    1. Should this include message types? Or rely on consumers to handle this?
    2. Does this need to include message types?



