using InterBus;
using Microsoft.Extensions.Logging;
using Sample.Components;

namespace Sample.AuditComponents;

public class EavesDroppingConsumer(ILogger<EavesDroppingConsumer> logger) : IConsumer<GreetingMessage>, IConsumer<FarewellMessage>
{
    public Task Consume(GreetingMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("[EavesDroppingConsumer] Received greeting message: {MessageText}", message.Text);
        return Task.CompletedTask;
    }

    public Task Consume(FarewellMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("[EavesDroppingConsumer] Received farewell message: {MessageText}", message.Text);
        return Task.CompletedTask;
    }
}
