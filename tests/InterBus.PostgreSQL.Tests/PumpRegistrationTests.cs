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
            Assert.That(source, Is.InstanceOf<IBufferedPollingSource>());
        Assert.That(provider.GetServices<IHostedService>().Count(service => service.GetType().Name == "PumpHost"), Is.EqualTo(1));
        Assert.That(provider.GetServices<IHostedService>().Any(service => service.GetType().Name == "PostgreSqlLeaseMessagePump"), Is.False);
    }
    [TestCase(PostgreSqlMessagePumpMode.Lease)]
    [TestCase(PostgreSqlMessagePumpMode.Transactional)]
    public async Task EachMode_RegistersOneFactoryAndCreatesIndependentAdapters(PostgreSqlMessagePumpMode mode)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddInterBus(bus => bus.AddPostgreSqlTransport(options =>
        {
            options.ConnectionString = "Host=localhost;Database=unused;Username=unused";
            options.MessagePumpMode = mode;
            options.PrefetchCount = 7;
        }));
        await using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetServices<IConsumerSourceFactory>().Count(), Is.EqualTo(1));
        var factory = provider.GetRequiredService<IConsumerSourceFactory>();
        var descriptor = new ConsumerDescriptor(typeof(PumpRegistrationTests), new Uri("pgsqlqueue://test"));
        await using var first = factory.Create(descriptor);
        await using var second = factory.Create(descriptor);
        Assert.That(first, Is.Not.SameAs(second));
        if (mode == PostgreSqlMessagePumpMode.Lease)
            Assert.That(((IBufferedPollingSource)first).PrefetchCount, Is.EqualTo(7));
        else Assert.That(first, Is.InstanceOf<IScopedPollingSource>());
    }

    [Test]
    public void LeasePrefetch_DefaultsToZero_AndRejectsNegativeValues()
    {
        Assert.That(new PostgreSqlTransportOptions().PrefetchCount, Is.Zero);
        var services = new ServiceCollection();
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddInterBus(bus =>
            bus.AddPostgreSqlTransport(options =>
            {
                options.ConnectionString = "Host=localhost;Database=unused;Username=unused";
                options.PrefetchCount = -1;
            })));
    }

}
