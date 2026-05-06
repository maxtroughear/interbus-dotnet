using InterBus.DynamoDB.Sagas;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace InterBus.DynamoDB.Extensions;

/// <summary>
/// Extension methods for configuring a DynamoDB-backed saga repository on an
/// <see cref="ISagaBuilder{TSaga, TState}"/>.
/// </summary>
public static class DynamoDbSagaBuilderExtensions
{
    /// <summary>
    /// Registers <see cref="DynamoDbSagaRepository{TSaga,TState}"/> as the
    /// <see cref="ISagaRepository{TSaga,TState}"/> for this saga.
    /// A no-op if an <see cref="ISagaRepository{TSaga,TState}"/> has already been registered.
    /// </summary>
    /// <remarks>
    /// The caller is responsible for registering <c>IAmazonDynamoDB</c> with the DI container
    /// (e.g. via <c>builder.Services.AddAWSService&lt;IAmazonDynamoDB&gt;()</c>).
    /// </remarks>
    /// <param name="builder">The saga builder.</param>
    /// <param name="configure">Optional delegate to customise <see cref="DynamoDbSagaOptions"/>.</param>
    public static ISagaBuilder<TSaga, TState> UseDynamoDbRepository<TSaga, TState>(
        this ISagaBuilder<TSaga, TState> builder,
        Action<DynamoDbSagaOptions>? configure = null)
        where TSaga : class, ISaga<TState>
        where TState : class, new()
    {
        var sagaOptions = new DynamoDbSagaOptions();
        configure?.Invoke(sagaOptions);

        builder.Services.TryAddSingleton(sagaOptions);
        builder.Services.TryAddSingleton<ISagaRepository<TSaga, TState>,
            DynamoDbSagaRepository<TSaga, TState>>();

        if (sagaOptions.CreateTableOnStartup)
            builder.Services.AddHostedService<DynamoDbSagaTableInitializer>();

        return builder;
    }
}
