using System.Text.Json.Serialization;

namespace InterBus.EntityFrameworkCore.Internal;

[JsonSerializable(typeof(Dictionary<string, string>))]
internal partial class OutboxJsonContext : JsonSerializerContext
{
}
