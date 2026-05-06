namespace InterBus.DynamoDB.Sagas;

/// <summary>
/// Configuration options for <see cref="DynamoDbSagaRepository{TSaga,TState}"/>.
/// </summary>
public sealed class DynamoDbSagaOptions
{
    /// <summary>
    /// The name of the DynamoDB table used to store saga state.
    /// The table must have a partition key named <c>CorrelationId</c> (string).
    /// </summary>
    public string TableName { get; set; } = "InterBusSagas";

    /// <summary>
    /// The duration for which a lock lease is held. Once this window elapses the
    /// lock is considered abandoned and may be acquired by another caller.
    /// Defaults to 30 seconds.
    /// </summary>
    public TimeSpan LockLeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long to wait between retries when trying to acquire a lock.
    /// Defaults to 200 milliseconds.
    /// </summary>
    public TimeSpan LockRetryInterval { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Maximum time to spend attempting to acquire a lock before throwing.
    /// Defaults to 60 seconds.
    /// </summary>
    public TimeSpan LockAcquireTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// When <c>true</c>, the DynamoDB saga table is automatically created on application startup
    /// if it does not already exist. Defaults to <c>false</c>.
    /// <para>
    /// The table is created with partition key <c>CorrelationId</c> (String) and sort key
    /// <c>SagaType</c> (String), using on-demand billing mode.
    /// </para>
    /// <para>
    /// <b>This option is intended for development and testing only.</b> For production
    /// environments, prefer managing DynamoDB tables with an Infrastructure-as-Code tool
    /// such as Terraform, AWS CloudFormation, or AWS CDK.
    /// </para>
    /// </summary>
    public bool CreateTableOnStartup { get; set; }
}
