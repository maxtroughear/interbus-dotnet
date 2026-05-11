using System.Text.Json.Serialization;

namespace InterBus.AmazonSqs;

[JsonSerializable(typeof(InterBusEnvelopeBody))]
[JsonSerializable(typeof(SnsNotificationEnvelopeBody))]
[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal sealed partial class SnsSqsJsonContext : JsonSerializerContext;
