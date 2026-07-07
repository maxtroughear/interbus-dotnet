using InterBus.EntityFrameworkCore.Sagas;
using InterBus.Sagas;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.EntityFrameworkCore.Extensions;

/// <summary>Extension methods for configuring the EF Core saga repository on an <see cref="SagaBuilder{TSaga, TState}"/>.</summary>
public static class EfCoreSagaBuilderExtensions
{
    /// <param name="builder">The saga builder.</param>
    /// <typeparam name="TSaga">The saga type.</typeparam>
    /// <typeparam name="TState">The saga state type.</typeparam>
    extension<TSaga, TState>(SagaBuilder<TSaga, TState> builder)
        where TSaga : class, ISaga<TState> where TState : class, new()
    {
        /// <summary>
        /// Registers <see cref="EfCoreSagaRepository{TSaga, TState, TDbContext}"/> as the
        /// <see cref="ISagaRepository{TSaga, TState}"/> for this saga, persisting state to the
        /// supplied <typeparamref name="TDbContext"/>.
        /// <para>
        /// <typeparamref name="TDbContext"/> must call <c>modelBuilder.AddInterBusSagas()</c>
        /// inside <c>OnModelCreating</c>.
        /// </para>
        /// </summary>
        /// <typeparam name="TDbContext">The application's <see cref="DbContext"/></typeparam>
        public SagaBuilder<TSaga, TState> UseEfCoreRepository<TDbContext>() where TDbContext : DbContext
        {
            builder.Services.TryAddScoped<InboxLoopConsumeState>();
            builder.UseConsumeMiddleware<TransactionConsumeMiddleware<TDbContext>>();

            builder.Services
                .TryAddScoped<ISagaRepository<TSaga, TState>, EfCoreSagaRepository<TSaga, TState, TDbContext>>();

            return builder;
        }
    }
}
