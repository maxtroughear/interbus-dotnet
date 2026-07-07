using InterBus.DeadLettering;
using Xunit;

namespace InterBus.Tests.DeadLettering;

public class SerializedExceptionTests
{
    [Fact]
    public void FromException_CapturesTypeMessageAndStackTrace()
    {
        Exception ex;
        try
        {
            throw new InvalidOperationException("test message");
        }
        catch (Exception e)
        {
            ex = e;
        }

        var result = SerializedException.FromException(ex);

        Assert.Equal("System.InvalidOperationException", result.ExceptionType);
        Assert.Equal("test message", result.Message);
        Assert.NotNull(result.StackTrace);
        Assert.Null(result.InnerException);
    }

    [Fact]
    public void FromException_WithInnerException_RecursivelyCapturesChain()
    {
        var inner = new FormatException("inner message");
        var outer = new InvalidOperationException("outer message", inner);

        var result = SerializedException.FromException(outer);

        Assert.Equal("System.InvalidOperationException", result.ExceptionType);
        Assert.Equal("outer message", result.Message);
        Assert.NotNull(result.InnerException);
        Assert.Equal("System.FormatException", result.InnerException.ExceptionType);
        Assert.Contains("inner message", result.InnerException.Message);
        Assert.Null(result.InnerException.InnerException);
    }

    [Fact]
    public void FromException_WithDeeplyNestedInnerExceptions_CapturesFullChain()
    {
        var level3 = new Exception("level 3");
        var level2 = new Exception("level 2", level3);
        var level1 = new Exception("level 1", level2);

        var result = SerializedException.FromException(level1);

        Assert.Equal("level 1", result.Message);
        Assert.NotNull(result.InnerException);
        Assert.Equal("level 2", result.InnerException.Message);
        Assert.NotNull(result.InnerException.InnerException);
        Assert.Equal("level 3", result.InnerException.InnerException.Message);
        Assert.Null(result.InnerException.InnerException.InnerException);
    }
}
