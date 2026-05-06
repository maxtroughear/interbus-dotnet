using System.Text.Json.Serialization;

namespace InterBus.DynamoDB.Outbox.Internal;

[JsonSerializable(typeof(Dictionary<string, string>))]
internal partial class DynamoDbOutboxJsonContext : JsonSerializerContext;
