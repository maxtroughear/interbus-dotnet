namespace InterBus.S3;

/// <summary>
/// Well-known header keys for the InterBus S3 claim check.
/// </summary>
public static class S3Headers
{
    /// <summary>
    /// Header key for the S3 claim check. When present on an incoming message, the payload
    /// has been stored in S3 and the value is the S3 object key. The dispatch middleware
    /// reads this header, fetches the original payload from S3, and replaces the message
    /// payload before dispatch.
    /// </summary>
    public const string ClaimCheckKey = "interbus-s3-claim-check";
}
