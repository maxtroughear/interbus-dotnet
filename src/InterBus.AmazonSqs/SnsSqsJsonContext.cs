using System.Text.Json.Serialization;

namespace InterBus.AmazonSqs;

[JsonSerializable(typeof(SnsNotificationEnvelopeBody))]
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(Dictionary<string, string[]>))]
[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal sealed partial class SnsSqsJsonContext : JsonSerializerContext;
