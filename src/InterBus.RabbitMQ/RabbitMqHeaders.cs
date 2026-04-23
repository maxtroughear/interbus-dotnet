namespace InterBus.RabbitMQ;

internal static class RabbitMqHeaders
{
    internal static Dictionary<string, object?> BuildAmqpHeaders(MessageHeaders headers)
    {
        var result = new Dictionary<string, object?>(headers.Count, StringComparer.Ordinal);
        foreach (var kv in headers)
            result[kv.Key] = kv.Value;
        return result;
    }

    internal static MessageHeaders ParseAmqpHeaders(IDictionary<string, object?>? amqpHeaders)
    {
        var dict = new Dictionary<string, string>(StringComparer.Ordinal);

        if (amqpHeaders is null)
            return new MessageHeaders(dict);

        foreach (var kv in amqpHeaders)
        {
            var value = kv.Value switch
            {
                byte[] bytes => System.Text.Encoding.UTF8.GetString(bytes),
                string str => str,
                _ => kv.Value?.ToString(),
            };

            if (value is not null)
                dict[kv.Key] = value;
        }

        return new MessageHeaders(dict);
    }
}
