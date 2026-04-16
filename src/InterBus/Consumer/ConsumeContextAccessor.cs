namespace InterBus.Consumer;

/// <inheritdoc cref="IConsumeContextAccessor"/>
internal sealed class ConsumeContextAccessor : IConsumeContextAccessor
{
    /// <inheritdoc/>
    public IConsumeContext? ConsumeContext { get; set; }
}
