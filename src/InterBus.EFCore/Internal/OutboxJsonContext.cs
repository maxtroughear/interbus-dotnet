using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace InterBus.EFCore.Internal;

[JsonSerializable(typeof(Dictionary<string, string>))]
internal partial class OutboxJsonContext : JsonSerializerContext
{
}
