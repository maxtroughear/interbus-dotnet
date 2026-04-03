namespace InterBus.RabbitMQ;

/// <summary>
/// Configuration options for the RabbitMQ (AMQP) transport.
/// </summary>
public sealed class RabbitMqTransportOptions
{
    /// <summary>
    /// RabbitMQ broker hostname. Ignored when <see cref="Uri"/> is set.
    /// Defaults to <c>localhost</c>.
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// RabbitMQ broker port. Ignored when <see cref="Uri"/> is set.
    /// Defaults to <c>5672</c> (AMQP default).
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// RabbitMQ virtual host. Ignored when <see cref="Uri"/> is set.
    /// Defaults to <c>/</c>.
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// RabbitMQ username. Ignored when <see cref="Uri"/> is set.
    /// Defaults to <c>guest</c>.
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// RabbitMQ password. Ignored when <see cref="Uri"/> is set.
    /// Defaults to <c>guest</c>.
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Optional AMQP connection URI (e.g. <c>amqp://user:pass@host:5672/vhost</c>).
    /// When set, overrides <see cref="HostName"/>, <see cref="Port"/>,
    /// <see cref="VirtualHost"/>, <see cref="UserName"/>, and <see cref="Password"/>.
    /// </summary>
    public string? Uri { get; set; }

    /// <summary>
    /// Prefix applied to RabbitMQ exchange names for topic (fan-out) destinations.
    /// Defaults to <c>interbus.topic.</c>.
    /// </summary>
    public string ExchangePrefix { get; set; } = "interbus.topic.";
}
