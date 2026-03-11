using InterBus;

namespace Sample.Components;

// ReSharper disable ClassNeverInstantiated.Global
public class GreetingConsumer(IMessagePublisher publisher) : IConsumer<GreetingMessage>
{
    public async Task Consume(GreetingMessage message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[GreetingConsumer] Received message: {message.Text}");

        var farewell = new FarewellMessage($"Goodbye from In-Memory Transport!");
        Console.WriteLine($"[GreetingConsumer] Publishing farewell message: {farewell.Text}");

        await publisher.Publish(farewell, cancellationToken);
    }
}

public class FarewellConsumer : IConsumer<FarewellMessage>
{
    public Task Consume(FarewellMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[FarewellConsumer] Received farewell message: {message.Text}");
        return Task.CompletedTask;
    }
}
