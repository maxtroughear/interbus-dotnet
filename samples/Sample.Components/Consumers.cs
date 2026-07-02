using InterBus;
using Microsoft.Extensions.Logging;

namespace Sample.Components;

// ReSharper disable ClassNeverInstantiated.Global
public class GreetingConsumer(ILogger<GreetingConsumer> logger, IPublisher publisher) : IConsumer<GreetingMessage>
{
    public async Task Consume(GreetingMessage message, IConsumeContext context,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation("[GreetingConsumer] Received message: {MessageText}", message.Text);

        var farewell = new FarewellMessage("Goodbye from greeting consumer!");
        logger.LogInformation("[GreetingConsumer] Publishing farewell message: {MessageText}", farewell.Text);

        await publisher.PublishAsync(farewell, cancellationToken);
    }
}

public class FarewellConsumer(ILogger<FarewellConsumer> logger) : IConsumer<FarewellMessage>
{
    public Task Consume(FarewellMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("[FarewellConsumer] Received message: {MessageText}", message.Text);
        return Task.CompletedTask;
    }
}

public class EchoConsumer(ILogger<EchoConsumer> logger, IPublisher publisher) : IConsumer<GreetingMessage>
{
    public Task Consume(GreetingMessage message, IConsumeContext context, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("[EchoConsumer] Received message: {MessageText}", message.Text);

        if (context.ReplyTo is not null)
        {
            logger.LogInformation("[EchoConsumer] Replying to {ReplyTo}", context.ReplyTo);
            return publisher.SendAsync(context.ReplyTo, message, cancellationToken);
        }

        logger.LogInformation("[EchoConsumer] No reply-to address found, not sending a reply");
        return Task.CompletedTask;
    }
}
