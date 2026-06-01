using InterBus.Consuming;
using Microsoft.EntityFrameworkCore;

namespace InterBus.EntityFrameworkCore;

internal class TransactionConsumeMiddleware<TDbContext>(TDbContext dbContext, InboxLoopConsumeState state)
    : IConsumeMiddleware
    where TDbContext : DbContext
{
    public async Task InvokeAsync<T>(ConsumeContext<T> context, Func<ConsumeContext<T>, CancellationToken, Task> next,
        CancellationToken cancellationToken = default) where T : class
    {
        do
        {
            await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);

            await next(context, cancellationToken);

            await transaction.CommitAsync(cancellationToken);
        } while (state.Loop);
    }
}