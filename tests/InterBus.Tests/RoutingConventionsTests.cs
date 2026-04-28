using InterBus.DeadLettering;
using Xunit;

namespace InterBus.Tests;

public class RoutingConventionsTests
{
    [Fact]
    public void TopicName_NonGenericType_ReturnsFullName()
    {
        var name = RoutingConventions.TopicName(typeof(SimpleMessage));
        Assert.Equal("InterBus.Tests.RoutingConventionsTests+SimpleMessage", name);
    }

    [Fact]
    public void TopicName_GenericType_ReturnsAngleBracketFormat()
    {
        var name = RoutingConventions.TopicName(typeof(DeadLetter<SimpleMessage>));
        Assert.Equal(
            "InterBus.DeadLettering.DeadLetter<InterBus.Tests.RoutingConventionsTests+SimpleMessage>",
            name);
    }

    [Fact]
    public void TopicName_NestedGenericType_ReturnsFullyQualifiedAngleBracketFormat()
    {
        var name = RoutingConventions.TopicName(typeof(DeadLetter<DeadLetter<SimpleMessage>>));
        Assert.Equal(
            "InterBus.DeadLettering.DeadLetter<InterBus.DeadLettering.DeadLetter<InterBus.Tests.RoutingConventionsTests+SimpleMessage>>",
            name);
    }

    private sealed record SimpleMessage;
}
