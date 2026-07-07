using InterBus.DynamoDB.Sagas;
using InterBus.Sagas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.DynamoDB.Extensions;

/// <summary>
/// Extension methods for configuring a DynamoDB-backed saga repository on an
/// <see cref="SagaBuilder{TSaga, TState}"/>.
/// </summary>
public static class DynamoDbSagaBuilderExtensions
{
    /// <param name="builder">The saga builder.</param>
    extension<TSaga, TState>(SagaBuilder<TSaga, TState> builder)
        where TSaga : class, ISaga<TState> where TState : class, new()
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
        /// <param name="configure">Optional delegate to customise <see cref="DynamoDbSagaOptions{TSaga}"/>.</param>
        public SagaBuilder<TSaga, TState> UseDynamoDbRepository(Action<DynamoDbSagaBuilder<TSaga>>? configure = null)
        {
            var sagaBuilder = new DynamoDbSagaBuilder<TSaga>(builder.Services);
            configure?.Invoke(sagaBuilder);

            builder.Services.TryAddSingleton(sagaBuilder.Options);
            builder.Services.TryAddScoped<ISagaRepository<TSaga, TState>, DynamoDbSagaRepository<TSaga, TState>>();
            builder.Services.TryAddScoped<IDynamoDbStateWriter<TSaga>, DynamoDbStateWriter<TSaga>>();

            if (sagaBuilder.Options.CreateTableOnStartup)
                builder.Services.AddHostedService<DynamoDbSagaTableInitializer<TSaga>>();

            return builder;
        }
    }
}
