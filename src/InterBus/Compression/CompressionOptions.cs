using System.IO.Compression;

namespace InterBus.Compression;

/// <summary>
/// Options for payload compression middleware.
/// </summary>
public sealed class CompressionOptions
{
    /// <summary>
    /// Minimum payload size in bytes required before compression is attempted.
    /// </summary>
    public int SizeThresholdBytes { get; set; } = 1_024;

    /// <summary>
    /// Compression level used when writing GZip payloads.
    /// </summary>
    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Fastest;
}
