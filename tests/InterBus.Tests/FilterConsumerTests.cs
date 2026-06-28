using InterBus.Filters;
using Xunit;

namespace InterBus.Tests;

public class FilterConsumerTests
{
    [Fact]
    public async Task FilterConsumer_Should_FilterOut_IgnoredMessages()
    {
        var filterMiddleware = new FilterConsumeMiddleware<TestMessage>(message => message.Value == "match");

        await filterMiddleware.InvokeAsync(TestConsumeContextFactory.Create(new TestMessage("no-match")),
            (_, _) =>
            {
                // This should not be called for the non-matching message
                Assert.Fail("This should not be called for non-matching messages");
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task FilterConsumer_Should_Allow_MatchingMessages()
    {
        var filterMiddleware = new FilterConsumeMiddleware<TestMessage>(message => message.Value == "match");

        var called = false;

        await filterMiddleware.InvokeAsync(TestConsumeContextFactory.Create(new TestMessage("match")),
            (_, _) =>
            {
                called = true;
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);

        Assert.True(called, "This should be called for matching messages");
    }

    [Fact]
    public async Task FilterConsumer_Allows_OtherMessageTypes()
    {
        var filterMiddleware = new FilterConsumeMiddleware<TestMessage>(message => message.Value == "match");

        var called = false;

        await filterMiddleware.InvokeAsync(TestConsumeContextFactory.Create(new OtherMessage()),
            (_, _) =>
            {
                called = true;
                return Task.CompletedTask;
            }, TestContext.Current.CancellationToken);

        Assert.True(called, "This should be called for other message types");
    }

    private sealed record TestMessage(string Value);

    private sealed record OtherMessage;
}
