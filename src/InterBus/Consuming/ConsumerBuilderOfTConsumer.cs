using InterBus.Dispatching;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Consuming;

public class ConsumerBuilder<TConsumer>(ConsumerDescriptor descriptor, IServiceCollection services) : ConsumerBuilder(descriptor, services)
    where TConsumer : class, IConsumer
{
    public ConsumerBuilder<TConsumer> Consumes<TMessageConsumer, TMessage>()
        where TMessageConsumer : class, TConsumer, IConsumer<TMessage> where TMessage : class => Consumes<TMessageConsumer, TMessage>(RoutingConventions.MessageTypeString(typeof(TMessage)));
    public ConsumerBuilder<TConsumer> Consumes<TMessageConsumer, TMessage>(string messageTypeName)
        where TMessageConsumer : class, TConsumer, IConsumer<TMessage> where TMessage : class
    {
        Descriptor.MessageTypes.Add(typeof(TMessage));
        Descriptor.SetRoute(messageTypeName, static (context, pipeline, ct) => pipeline.ExecuteAsync<TMessageConsumer, TMessage>(context, ct));
        Services.TryAddScoped<IConsumerExecutor<TMessageConsumer, TMessage>, ConsumerExecutor<TMessageConsumer, TMessage>>();
        return this;
    }

    public ConsumerBuilder<TConsumer> MessageTypeFromHeader(string headerName)
    {
        Descriptor.MessageTypeHeader = headerName;
        return this;
    }

    public ConsumerBuilder<TConsumer> Dispatch(Func<DispatchContext, ConsumePipelineExecutor, IServiceProvider, CancellationToken, Task> dispatch)
    {
        Descriptor.Dispatch = dispatch;
        return this;
    }

    /// <inheritdoc cref = "ConsumerBuilder.WithFifo"/>
    public new ConsumerBuilder<TConsumer> WithFifo()
    {
        base.WithFifo();
        return this;
    }
}
