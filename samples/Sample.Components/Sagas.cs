using InterBus;
using InterBus.Sagas;

namespace Sample.Components;

public record CheckGreeting(Guid Id);

public class GreetingSagaState
{
    public string Greeting { get; set; } = string.Empty;
}

public class GreetingSaga(IPublisher publisher) : ISagaConsumer<GreetingSagaState, GreetingMessage>, ISagaConsumer<GreetingSagaState, CheckGreeting>
{
    public string GetCorrelationId(GreetingMessage message) => message.Id.ToString();

    public Task Consume(GreetingMessage message, ISagaContext<GreetingSagaState> context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[GreetingSaga] Received greeting message: {message.Text}");
        context.State.Greeting = message.Text;
        
        return publisher.PublishAsync(new CheckGreeting(message.Id), cancellationToken);
    }

    public string GetCorrelationId(CheckGreeting message) => message.Id.ToString();

    public Task Consume(CheckGreeting message, ISagaContext<GreetingSagaState> context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[GreetingSaga] Current greeting for ID {message.Id}: {context.State.Greeting}");
        context.Complete();
        return Task.CompletedTask;
    }
}
