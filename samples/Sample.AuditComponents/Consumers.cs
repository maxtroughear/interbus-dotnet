using InterBus;
using Sample.Components;

namespace Sample.AuditComponents;

public class EavesDroppingConsumer : IConsumer<GreetingMessage>, IConsumer<FarewellMessage>
{
    public Task Consume(GreetingMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[EavesDroppingConsumer] Received greeting message: {message.Text}");
        return Task.CompletedTask;
    }

    public Task Consume(FarewellMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"[EavesDroppingConsumer] Received farewell message: {message.Text}");
        return Task.CompletedTask;
    }
}

