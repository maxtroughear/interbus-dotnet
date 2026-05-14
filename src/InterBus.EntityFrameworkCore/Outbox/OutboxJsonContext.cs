using System.Text.Json.Serialization;

namespace InterBus.EntityFrameworkCore.Outbox;

[JsonSerializable(typeof(Dictionary<string, string>))]
internal partial class OutboxJsonContext : JsonSerializerContext;
