using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace InterBus.Extensions;

/// <summary>Extension methods for <see cref="IInterBusBuilder"/>.</summary>
public static class InterBusBuilderExtensions
{
    /// <summary>
    /// Register a <see cref="SystemTextJsonMessageSerializer"/> configured with the provided
    /// <paramref name="options"/>, replacing the default reflection-based serialiser.
    /// <para>
    /// Designed for trimmed or Native AOT applications where a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> must be supplied:
    /// </para>
    /// <code>
    /// bus.UseSystemTextJsonSerializer(MyAotJsonContext.Default.Options);
    /// </code>
    /// </summary>
    /// <param name="builder">The InterBus builder.</param>
    /// <param name="options">
    /// <see cref="JsonSerializerOptions"/> to pass to <see cref="SystemTextJsonMessageSerializer"/>.
    /// Typically <c>MyJsonContext.Default.Options</c> from a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>.
    /// </param>
    public static IInterBusBuilder UseSystemTextJsonSerializer(
        this IInterBusBuilder builder,
        JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);

        builder.Services.Replace(
            ServiceDescriptor.Singleton<IMessageSerializer>(
                new SystemTextJsonMessageSerializer(options)));
        return builder;
    }
}
