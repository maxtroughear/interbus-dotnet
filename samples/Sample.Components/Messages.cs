namespace Sample.Components;

public record GreetingMessage(string Text)
{
    public Guid Id = Guid.NewGuid();
}

public record FarewellMessage(string Text);
