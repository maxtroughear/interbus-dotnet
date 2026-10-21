using InterBus.Consuming;
using InterBus.MessagePumps;
using Microsoft.Extensions.DependencyInjection;

namespace InterBus.PostgreSQL;

internal sealed class PostgreSqlConsumerSourceFactory(IServiceProvider provider, PostgreSqlTransportOptions options) : IConsumerSourceFactory
{
    public IConsumerSource Create(ConsumerDescriptor descriptor) => options.MessagePumpMode == PostgreSqlMessagePumpMode.Lease ? ActivatorUtilities.CreateInstance<PostgreSqlLeaseMessagePump>(provider, descriptor) : ActivatorUtilities.CreateInstance<PostgreSqlTransactionalMessagePump>(provider, descriptor);
}
