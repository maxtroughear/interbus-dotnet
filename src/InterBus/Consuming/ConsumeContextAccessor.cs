namespace InterBus.Consuming;

/// <inheritdoc cref="IConsumeContextAccessorOld"/>
internal sealed class ConsumeContextAccessorOldOld : IConsumeContextAccessorOld
{
    /// <inheritdoc/>
    public IConsumeContext? ConsumeContext { get; set; }
}

public sealed class ConsumeContextAccessor
{
    public ConsumeContext? ConsumeContext { get; internal set; }
}
