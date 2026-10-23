using InterBus.Consuming;
using InterBus.Dispatching;
using InterBus.Extensions;
using InterBus.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace InterBus.Tests.Resilience;

public sealed class PumpCircuitConfigurationTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ConflictingMiddleware_IsRejectedInEitherOrder(bool pumpFirst)
    {
        var builder = new ConsumerBuilder(new ConsumerDescriptor(typeof(PumpCircuitTests), new Uri("memory://test")), new ServiceCollection());
        if (pumpFirst)
        {
            builder.UsePumpCircuitBreaker();
            Assert.Throws<InvalidOperationException>(() => builder.UseCircuitBreakerMiddleware());
        }
        else
        {
            builder.UseCircuitBreakerMiddleware();
            Assert.Throws<InvalidOperationException>(() => builder.UsePumpCircuitBreaker());
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DirectMiddlewareRegistration_IsValidatedWithoutBuilderState(bool pumpFirst)
    {
        var services = new ServiceCollection();
        Assert.Throws<InvalidOperationException>(() => services.AddInterBus(bus =>
            bus.RegisterConsumer<ConfigurationConsumer>(consumer =>
            {
                if (pumpFirst) consumer.UsePumpCircuitBreaker();
                consumer.UseDispatchMiddleware<CircuitBreakerDispatchMiddleware>();
                if (!pumpFirst) consumer.UsePumpCircuitBreaker();
            })));
    }

    [Fact]
    public void DifferentConsumers_CanUseDifferentCircuitFeatures()
    {
        var services = new ServiceCollection();
        var dispatch = new ConsumerBuilder(new ConsumerDescriptor(typeof(ConfigurationConsumer), new Uri("memory://dispatch")), services);
        var pump = new ConsumerBuilder(new ConsumerDescriptor(typeof(PumpCircuitTests), new Uri("memory://pump")), services);
        dispatch.UseCircuitBreakerMiddleware();
        pump.UsePumpCircuitBreaker();
        Assert.NotNull(pump.Descriptor.PumpCircuitBreaker);
    }

    [Fact]
    public void FluentPolicies_RetainSettingsAcrossConfigurationCalls()
    {
        var options = new PumpCircuitBreakerOptions();
        var settings = new CircuitBreakerOptions[2];
        Assert.Same(options, options.Dependency(policy => { policy.MinimumThroughput = 7; settings[0] = policy; })
            .Transport(policy => { policy.MinimumThroughput = 9; settings[1] = policy; }));
        options.Dependency(policy => Assert.Same(settings[0], policy));
        options.Transport(policy => Assert.Same(settings[1], policy));
        Assert.Equal(7, settings[0].MinimumThroughput);
        Assert.Equal(9, settings[1].MinimumThroughput);
        options.DisableDependency().DisableTransport();
        options.Dependency(policy => Assert.NotSame(settings[0], policy));
        options.Transport(policy => Assert.NotSame(settings[1], policy));
    }

    private sealed class ConfigurationConsumer : IConsumer;

    [Fact]
    public void InvalidConfiguration_IsRejected()
    {
        var builder = new ConsumerBuilder(new ConsumerDescriptor(typeof(PumpCircuitTests), new Uri("memory://test")), new ServiceCollection());
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.UsePumpCircuitBreaker(o => o.Dependency(policy => policy.FailureThreshold = double.NaN)));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.UsePumpCircuitBreaker(o => o.SuccessfulProbesRequired = 2));
        Assert.Throws<ArgumentException>(() => builder.UsePumpCircuitBreaker(o => o.DisableDependency()));
    }

}
