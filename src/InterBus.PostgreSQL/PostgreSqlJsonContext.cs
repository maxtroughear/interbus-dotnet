using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace InterBus.PostgreSQL;

[JsonSerializable(typeof(Dictionary<string, string>))]
internal partial class PostgreSqlJsonContext : JsonSerializerContext
{
}
