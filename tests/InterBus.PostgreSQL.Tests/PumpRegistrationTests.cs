using InterBus.Consuming;
using InterBus.Extensions;
using InterBus.MessagePumps;
using InterBus.PostgreSQL.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace InterBus.PostgreSQL.Tests;

[TestFixture]
public class PumpRegistrationTests
{
    [Test]
    public async Task Lease_RegistersCoreHostAndBufferedSource()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus => bus.AddPostgreSqlTransport(options =>
        {
            options.ConnectionString = "Host=localhost;Database=unused;Username=unused";
            options.MessagePumpMode = PostgreSqlMessagePumpMode.Lease;
        }));
        await using var provider = services.BuildServiceProvider();
        var source = provider.GetRequiredService<IConsumerSourceFactory>().Create(
            new ConsumerDescriptor(typeof(PumpRegistrationTests), new Uri("pgsqlqueue://test")));
        await using (source)
            Assert.That(source, Is.InstanceOf<IBufferedSource>());
        Assert.That(provider.GetServices<IHostedService>().Count(service => service.GetType().Name == "PumpHost"), Is.EqualTo(1));
        Assert.That(provider.GetServices<IHostedService>().Any(service => service.GetType().Name == "PostgreSqlLeaseMessagePump"), Is.False);
    }
}
