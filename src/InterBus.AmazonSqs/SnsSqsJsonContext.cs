using System.Text.Json.Serialization;

namespace InterBus.AmazonSqs;

[JsonSerializable(typeof(SnsSqsMessageEnvelope))]
[JsonSerializable(typeof(SnsSqsNotificationEnvelope))]
[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal sealed partial class SnsSqsJsonContext : JsonSerializerContext;
