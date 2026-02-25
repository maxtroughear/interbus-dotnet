using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace InterBus;

/// <summary>
/// Runtime options populated by <see cref="IInterBusBuilder"/> during startup.
/// Consumed by transport implementations to initialise their topics and queues.
/// </summary>
public sealed class InterBusOptions
{
    private readonly Dictionary<string, ConsumerAccumulator> _accumulators = new(StringComparer.Ordinal);
    private readonly Dictionary<Type, string> _topicNames = [];
    private List<ConsumerDescriptor>? _built;

    /// <summary>
    /// All registered consumer descriptors. One descriptor per consumer class.
    /// This collection is built lazily the first time it is accessed (after all
    /// <c>AddConsumer</c> calls have been made during the <c>AddInterBus</c> configuration phase).
    /// </summary>
    public IReadOnlyList<ConsumerDescriptor> Consumers => _built
        ?? throw new InvalidOperationException(
            "InterBus consumers have not been finalized. Ensure AddInterBus is fully configured before accessing Consumers.");

    /// <summary>
    /// The <see cref="System.Text.Json.JsonSerializerOptions"/> used to serialize and deserialize
    /// message payloads. To enable trimming and AOT compatibility, configure this with a
    /// source-generated <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> via
    /// <see cref="IInterBusBuilder.Options"/> before calling <c>AddConsumer</c>.
    /// Defaults to a reflection-based resolver; override for trimmed or AOT applications.
    /// </summary>
    public JsonSerializerOptions SerializerOptions { get; set; }

    /// <summary>
    /// Initializes a new <see cref="InterBusOptions"/> with a reflection-based JSON resolver as the
    /// default. Override <see cref="SerializerOptions"/> via <c>bus.Options.SerializerOptions</c>
    /// with a source-generated <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>
    /// for trimmed or Native AOT applications.
    /// </summary>
    [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026",
        Justification =
            "DefaultJsonTypeInfoResolver uses reflection. Override SerializerOptions with a source-generated JsonSerializerContext for AOT/trim applications.")]
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050",
        Justification =
            "DefaultJsonTypeInfoResolver requires dynamic code. Override SerializerOptions with a source-generated JsonSerializerContext for AOT applications.")]
    public InterBusOptions()
    {
        SerializerOptions = new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
    }

    internal void SetTopicName(Type messageType, string topicName) => _topicNames[messageType] = topicName;

    internal bool TryGetTopicName(Type messageType, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? topicName) =>
        _topicNames.TryGetValue(messageType, out topicName);

    /// <summary>
    /// Accumulates a per-type handler for the given consumer. Called by
    /// <see cref="IInterBusBuilder.AddConsumer{TConsumer,T}"/> for each message type.
    /// </summary>
    internal void AccumulateConsumerHandler(
        Type consumerType,
        string queueName,
        string topicName,
        string messageTypeFullName,
        Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> handler)
    {
        if (!_accumulators.TryGetValue(queueName, out var acc))
        {
            acc = new ConsumerAccumulator(consumerType, queueName);
            _accumulators[queueName] = acc;
        }

        acc.AddTypeHandler(topicName, messageTypeFullName, handler);
    }

    /// <summary>
    /// Sets a pre-built dispatch delegate for the given consumer. Called by
    /// <see cref="IInterBusBuilder.AddConsumer{TConsumer}(Type[],Func{IServiceProvider,string,MessageHeaders,CancellationToken,Task})"/>
    /// with source-generated dispatch code.
    /// </summary>
    internal void SetConsumerDispatch(
        Type consumerType,
        string queueName,
        IReadOnlyList<string> topicNames,
        Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> dispatch)
    {
        _accumulators[queueName] = new ConsumerAccumulator(consumerType, queueName, topicNames, dispatch);
    }

    /// <summary>
    /// Called once by <see cref="Extensions.ServiceCollectionExtensions.AddInterBus"/> after the
    /// configure delegate returns. Builds all consumer descriptors from the accumulated handlers.
    /// </summary>
    internal void FinalizeConsumers()
    {
        _built = _accumulators.Values.Select(a => a.Build()).ToList();
    }

    // -------------------------------------------------------------------------
    // Internal accumulator — one per consumer class
    // -------------------------------------------------------------------------

    private sealed class ConsumerAccumulator
    {
        private readonly Type _consumerType;
        private readonly string _queueName;
        // For per-type (old API) accumulated handlers.
        private readonly List<string> _topicNames = [];
        private readonly Dictionary<string, Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task>>
            _handlersByMessageType = new(StringComparer.Ordinal);
        // For pre-built (source-gen) dispatch.
        private Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task>? _prebuiltDispatch;
        private IReadOnlyList<string>? _prebuiltTopicNames;

        public ConsumerAccumulator(Type consumerType, string queueName)
        {
            _consumerType = consumerType;
            _queueName = queueName;
        }

        /// <summary>Constructor for pre-built (source-generated) dispatch.</summary>
        public ConsumerAccumulator(
            Type consumerType,
            string queueName,
            IReadOnlyList<string> topicNames,
            Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> dispatch)
        {
            _consumerType = consumerType;
            _queueName = queueName;
            _prebuiltTopicNames = topicNames;
            _prebuiltDispatch = dispatch;
        }

        public void AddTypeHandler(
            string topicName,
            string messageTypeFullName,
            Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> handler)
        {
            _topicNames.Add(topicName);
            _handlersByMessageType[messageTypeFullName] = handler;
        }

        public ConsumerDescriptor Build()
        {
            // Source-generated pre-built dispatch takes priority.
            if (_prebuiltDispatch is not null)
                return new ConsumerDescriptor(_consumerType, _queueName, _prebuiltTopicNames!, _prebuiltDispatch);

            var topicNames = (IReadOnlyList<string>)_topicNames.AsReadOnly();
            var handlers = _handlersByMessageType; // captured by dispatch closure
            var consumerType = _consumerType;

            Func<IServiceProvider, string, MessageHeaders, CancellationToken, Task> dispatch =
                (sp, payload, headers, ct) =>
                {
                    if (!headers.TryGetValue(InterBusHeaders.MessageType, out var msgType))
                    {
                        sp.GetService<ILoggerFactory>()
                            ?.CreateLogger(consumerType.FullName!)
                            .LogError(
                                "InterBus: message received without {Header} header for consumer {Consumer}",
                                InterBusHeaders.MessageType, consumerType.Name);
                        return Task.CompletedTask;
                    }

                    if (handlers.TryGetValue(msgType, out var handler))
                        return handler(sp, payload, headers, ct);

                    sp.GetService<ILoggerFactory>()
                        ?.CreateLogger(consumerType.FullName!)
                        .LogError(
                            "InterBus: consumer {Consumer} does not handle message type {MessageType}; message ignored",
                            consumerType.Name, msgType);
                    return Task.CompletedTask;
                };

            return new ConsumerDescriptor(_consumerType, _queueName, topicNames, dispatch);
        }
    }
}
