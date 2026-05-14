using System.Text.Json.Serialization;

namespace InterBus.DynamoDB.Outbox;

[JsonSerializable(typeof(Dictionary<string, string>))]
internal partial class DynamoDbOutboxJsonContext : JsonSerializerContext;
