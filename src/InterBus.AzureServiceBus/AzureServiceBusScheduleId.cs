using System.Globalization;
using System.Text;

namespace InterBus.AzureServiceBus;

internal static class AzureServiceBusScheduleId
{
    public static string Encode(string entityPath, long sequenceNumber) =>
        $"asb:v1:{Convert.ToBase64String(Encoding.UTF8.GetBytes(entityPath)).TrimEnd('=').Replace('+', '-').Replace('/', '_')}:{sequenceNumber.ToString(CultureInfo.InvariantCulture)}";

    public static (string EntityPath, long SequenceNumber) Decode(string scheduleId)
    {
        var parts = scheduleId?.Split(':');
        if (parts is { Length: 4 } && parts[0] == "asb" && parts[1] == "v1" &&
            long.TryParse(parts[3], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var sequence))
        {
            try
            {
                var encoded = parts[2].Replace('-', '+').Replace('_', '/');
                var path = new UTF8Encoding(false, true).GetString(Convert.FromBase64String(
                    encoded.PadRight((encoded.Length + 3) / 4 * 4, '=')));
                if (!string.IsNullOrWhiteSpace(path) && !path.Any(char.IsControl) &&
                    Encode(path, sequence) == scheduleId)
                    return (path, sequence);
            }
            catch (FormatException) { }
            catch (DecoderFallbackException) { }
        }
        throw new ArgumentException("Invalid Azure Service Bus schedule ID or unsupported version", nameof(scheduleId));
    }
}
