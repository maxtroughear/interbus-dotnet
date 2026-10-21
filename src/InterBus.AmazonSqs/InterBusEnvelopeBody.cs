using System.Text.Json.Serialization;

namespace InterBus.AmazonSqs;

internal sealed record InterBusEnvelopeBody([property: JsonPropertyName("p")] string Payload, [property: JsonPropertyName("h")] Dictionary<string, string> Headers);
