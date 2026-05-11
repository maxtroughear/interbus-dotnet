using InterBus.Sagas;

namespace Sample.Components;

public class GreetingSagaState
{
    
}

public class GreetingSaga : ISagaConsumer<GreetingSagaState, GreetingMessage>
{
    public string GetCorrelationId(GreetingMessage message) => message.Text;

    public Task Consume(GreetingMessage message, ISagaContext<GreetingSagaState> context, CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}
