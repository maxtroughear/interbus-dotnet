using System.Security.Cryptography;
using System.Text;

namespace InterBus.AzureServiceBus;

/// <summary>
/// Utility methods for sanitizing and constraining Azure Service Bus entity names.
/// </summary>
internal static class AzureServiceBusNaming
{
    /// <summary>
    /// Maximum length of an Azure Service Bus subscription name.
    /// </summary>
    internal const int MaxSubscriptionNameLength = 50;

    /// <summary>
    /// Sanitizes a name for use as an Azure Service Bus topic or queue name.
    /// Replaces characters not permitted in entity names (<c>+</c>) with a hyphen.
    /// </summary>
    internal static string SanitizeEntityName(string name) =>
        name.Replace('+', '-');

    /// <summary>
    /// Sanitizes a name for use as an Azure Service Bus subscription name.
    /// Replaces <c>+</c> and <c>/</c> with hyphens, and truncates to
    /// <see cref="MaxSubscriptionNameLength"/> characters using a stable hash suffix when
    /// the sanitized name exceeds the limit.
    /// </summary>
    internal static string SanitizeSubscriptionName(string name)
    {
        var sanitized = name.Replace('+', '-').Replace('/', '-');

        if (sanitized.Length <= MaxSubscriptionNameLength)
            return sanitized;

        // Truncate: keep 41 chars, append '-', then 8 hex chars of SHA-256 hash.
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sanitized)));
        return sanitized[..41] + "-" + hash[..8].ToLowerInvariant();
    }
}
