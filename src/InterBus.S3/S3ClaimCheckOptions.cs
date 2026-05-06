namespace InterBus.S3;

/// <summary>
/// Options for the S3 claim check middleware.
/// </summary>
public sealed class S3ClaimCheckOptions
{
    /// <summary>
    /// The S3 bucket name where large message payloads are stored.
    /// </summary>
    public string BucketName { get; set; } = string.Empty;

    /// <summary>
    /// The size threshold in bytes above which the claim check is used.
    /// The threshold is applied to the total estimated message size: payload plus headers.
    /// Defaults to 240 KB to leave headroom for transport-level attributes and metadata.
    /// </summary>
    public int SizeThresholdBytes { get; set; } = 240 * 1024;

    /// <summary>
    /// Optional S3 key prefix applied to every stored payload object.
    /// Defaults to <c>interbus/</c>.
    /// </summary>
    public string KeyPrefix { get; set; } = "interbus/";
}
