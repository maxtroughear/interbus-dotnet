using System.Web;

namespace InterBus.Publishing;

/// <summary>
/// Fluent builder for per-message-type configuration. Obtained via
/// <c>builder.Configure&lt;T&gt;(c =&gt; { ... })</c> on <see cref="InterBusBuilder"/>.
/// </summary>
/// <typeparam name="T">The message type being configured.</typeparam>
public sealed class PublisherBuilder<T>(
    PublisherDescriptor descriptor,
    InterBusOptions options)
    where T : class
{
    /// <summary>
    /// Overrides the convention-based SNS/SQS topic name for <typeparamref name="T"/>.
    /// Equivalent to the now-obsolete <c>ConfigureTopic&lt;T&gt;(string)</c> on
    /// <see cref="InterBusBuilder"/>.
    /// </summary>
    public PublisherBuilder<T> TopicName(string topicName)
    {
        descriptor.Topic = RoutingConventions.TopicUri(options.TopicUriScheme, topicName);
        return this;
    }

    /// <summary>
    /// Declares <typeparamref name="T"/> as a FIFO message type. Transport implementations
    /// (e.g. Amazon SNS/SQS) will provision FIFO-capable infrastructure (`.fifo` topic/queue
    /// names, FIFO attributes, content-based deduplication).
    /// <para>
    /// The optional <paramref name="groupBy"/> selector is evaluated on each outgoing message
    /// to produce the <see cref="InterBusHeaders.MessageGroupId"/> header value. When omitted
    /// the message's <see cref="InterBusHeaders.CorrelationId"/> is used as the group ID.
    /// </para>
    /// <para>
    /// Both publisher and subscriber services should call <c>Fifo</c> for the same message type
    /// so that they agree on the FIFO topic name.
    /// </para>
    /// </summary>
    /// <param name="groupBy">
    /// Optional selector producing a stable group identifier from the message. Messages with
    /// the same group ID are delivered in order. Common choices: tenant ID, aggregate root ID,
    /// or any other partitioning key.
    /// </param>
    public PublisherBuilder<T> Fifo(Func<T, string>? groupBy = null)
    {
        descriptor.IsFifo = true;
        if (groupBy is not null)
            GroupBy(groupBy);

        var uriBuilder = new UriBuilder(descriptor.Topic);

        var queryMap = HttpUtility.ParseQueryString(uriBuilder.Query);
        queryMap["fifo"] = "true";
        uriBuilder.Query = queryMap.ToString();

        descriptor.Topic = uriBuilder.Uri;

        return this;
    }

    /// <summary>
    /// Registers a group ID selector for <typeparamref name="T"/> without declaring it as a
    /// FIFO message type. The selector is evaluated to populate the
    /// <see cref="InterBusHeaders.MessageGroupId"/> header on all outgoing messages of this type.
    /// <para>
    /// Use <see cref="Fifo"/> instead when you also want FIFO infrastructure provisioned.
    /// </para>
    /// </summary>
    public PublisherBuilder<T> GroupBy(Func<T, string> selector)
    {
        descriptor.GroupBy = msg => selector((T)msg);
        return this;
    }
}
