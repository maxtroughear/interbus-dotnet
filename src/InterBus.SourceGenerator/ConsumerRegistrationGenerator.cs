using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace InterBus.SourceGenerator;

/// <summary>
/// Incremental source generator that produces compile-time consumer registrations and dispatch
/// delegates for InterBus.
///
/// <para><b>Architecture - decoupled dispatch from registration:</b></para>
/// <list type="bullet">
///   <item><b>Per-assembly dispatch module</b> – emitted in every project that has
///   <c>IConsumer&lt;T&gt;</c> implementations, using <c>SyntaxProvider</c> so the step is
///   fast and incremental (only re-runs when class declarations change in the current project).
///   The generated <c>public static class InterBusConsumerRegistrations_{AssemblyName}</c>
///   contains the dispatch delegates, an <c>AddConsumers()</c> method, and a
///   <c>TryGetDispatch()</c> lookup method.</item>
///
///   <item><b>Aggregating registrations class</b> – emitted in every project that can see one
///   or more dispatch modules (either from the current project or from referenced assemblies).
///   This step scans <em>only</em> the <c>InterBus.Generated</c> namespace for
///   <c>InterBusConsumerRegistrations_*</c> types — a targeted, cheap scan rather than walking
///   all types in the global namespace.  The generated <c>internal static class
///   InterBusConsumerRegistrations</c> exposes <c>AddConsumers()</c> and
///   <c>AddConsumer&lt;TConsumer&gt;()</c> extension methods that cover all discovered
///   modules.</item>
/// </list>
///
/// <para>This design allows consumers defined in library assemblies (including NuGet packages)
/// to be registered in a host project without any runtime reflection, while keeping incremental
/// compilation fast.</para>
/// </summary>
[Generator]
public sealed class ConsumerRegistrationGenerator : IIncrementalGenerator
{
    private const string ConsumerInterfaceMetadataName = "InterBus.IConsumer`1";
    private const string GeneratedNamespace = "InterBus.Generated";

    // Prefix for per-assembly dispatch modules; also used to discover them in GlobalNamespace.
    private const string DispatchModulePrefix = "InterBusConsumerRegistrations_";
    private const string AggregatingClassName = "InterBusConsumerRegistrations";

    private const string DispatchFuncType =
        "global::System.Func<global::System.IServiceProvider, string, global::InterBus.IConsumeContext, global::System.Threading.CancellationToken, global::System.Threading.Tasks.Task>";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // Collect IConsumer<T> class symbols from this project's own syntax trees only.
        // SyntaxProvider is incremental: it only re-runs when class declarations change.
        var classSymbols = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => node is ClassDeclarationSyntax,
                transform: static (ctx, ct) =>
                    ctx.SemanticModel.GetDeclaredSymbol(ctx.Node, ct) as INamedTypeSymbol)
            .Where(static symbol => symbol is not null)
            .Select(static (symbol, _) => symbol!);

        // Combine with the full compilation for:
        //   (a) resolving IConsumer<T> interface symbol
        //   (b) discovering InterBusConsumerRegistrations_* modules already compiled into
        //       referenced assemblies (the decoupled cross-assembly discovery path).
        var combined = classSymbols.Collect().Combine(context.CompilationProvider);

        context.RegisterSourceOutput(combined, static (ctx, data) =>
            Execute(ctx, data.Left, data.Right));
    }

    private static void Execute(
        SourceProductionContext ctx,
        ImmutableArray<INamedTypeSymbol> syntaxClasses,
        Compilation compilation)
    {
        var consumerInterface = compilation.GetTypeByMetadataName(ConsumerInterfaceMetadataName);
        if (consumerInterface is null) return;

        // ── Step 1: IConsumer<T> implementations in this project's source trees ──────────────
        // Processes only the SyntaxProvider-derived symbols (current project, fast path).
        var currentConsumers = new List<(INamedTypeSymbol Consumer, List<INamedTypeSymbol> Messages)>();

        foreach (var classSymbol in syntaxClasses)
        {
            if (classSymbol.IsAbstract || classSymbol.TypeKind != TypeKind.Class) continue;

            List<INamedTypeSymbol>? messages = null;

            foreach (var symbol in classSymbol.AllInterfaces)
            {
                if (symbol.TypeArguments.Length != 1) continue;
                if (!SymbolEqualityComparer.Default.Equals(symbol.OriginalDefinition, consumerInterface)) continue;
                if (symbol.TypeArguments[0] is not INamedTypeSymbol messageType) continue;

                messages ??= new List<INamedTypeSymbol>();
                messages.Add(messageType);
            }

            if (messages is not null)
                currentConsumers.Add((classSymbol, messages));
        }

        // ── Step 2: Dispatch modules from referenced assemblies ──────────────────────────────
        // Looks only in the InterBus.Generated namespace for InterBusConsumerRegistrations_*
        // types — a targeted scan rather than walking all types in the global namespace.
        // These modules were generated by this same generator when the referenced assemblies
        // were compiled.
        var externalModuleNames = FindExternalDispatchModules(compilation);

        // Nothing to generate.
        if (currentConsumers.Count == 0 && externalModuleNames.Count == 0) return;

        var assemblyName = SanitizeAssemblyName(compilation.AssemblyName ?? "Unknown");
        var currentModuleName = DispatchModulePrefix + assemblyName;
        
        // AddConsumer<TConsumer>() must be able to find consumers from referenced assemblies,
        // so we still build the full module list for that method.
        var allModuleNames = new List<string>(externalModuleNames);
        if (currentConsumers.Count > 0)
            allModuleNames.Add(currentModuleName);

        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("using global::Microsoft.Extensions.DependencyInjection;");
        sb.AppendLine("using global::Microsoft.Extensions.Logging;");
        sb.AppendLine();
        sb.AppendLine($"namespace {GeneratedNamespace}");
        sb.AppendLine("{");

        // ── Part A: per-assembly dispatch module (only when this project has consumers) ───────
        if (currentConsumers.Count > 0)
        {
            AppendDispatchModule(sb, currentModuleName, currentConsumers);
            sb.AppendLine();
        }

        // ── Part B: aggregating registrations class ──────────────────────────────────────────
        AppendAggregatingClass(sb, allModuleNames);

        sb.AppendLine("}");

        ctx.AddSource(
            "InterBusConsumerRegistrations.g.cs",
            SourceText.From(sb.ToString(), Encoding.UTF8));
    }

    // ── Dispatch module ──────────────────────────────────────────────────────────────────────

    private static void AppendDispatchModule(
        StringBuilder sb,
        string moduleName,
        List<(INamedTypeSymbol Consumer, List<INamedTypeSymbol> Messages)> consumers)
    {
        sb.AppendLine("    /// <summary>");
        sb.AppendLine("    /// Source-generated per-assembly dispatch module.");
        sb.AppendLine("    /// Produced by <c>InterBus.SourceGenerator</c> for this assembly's");
        sb.AppendLine("    /// <c>IConsumer&lt;T&gt;</c> implementations. Consumed by the aggregating");
        sb.AppendLine($"    /// <c>{AggregatingClassName}</c> class. Not intended for direct use.");
        sb.AppendLine("    /// </summary>");
        sb.AppendLine("    [global::System.ComponentModel.EditorBrowsable(");
        sb.AppendLine("        global::System.ComponentModel.EditorBrowsableState.Never)]");
        sb.AppendLine($"    public static class {moduleName}");
        sb.AppendLine("    {");

        // Static dispatch table: consumer Type → (message types, dispatch delegate).
        sb.AppendLine("        private static readonly global::System.Collections.Generic.Dictionary<");
        sb.AppendLine("            global::System.Type,");
        sb.AppendLine($"            (global::System.Type[] MessageTypes, {DispatchFuncType} Dispatch)>");
        sb.AppendLine("            _dispatches = new()");
        sb.AppendLine("        {");

        foreach (var (consumer, messages) in consumers)
        {
            AppendDispatchBody(sb, consumer, messages);
            
        }

        sb.AppendLine("        };");
        sb.AppendLine();

        // AddConsumers(): statically typed AddConsumer<T> calls.
        // Called by the host's aggregating AddConsumers() extension method.
        sb.AppendLine("        /// <summary>Adds all consumers defined in this assembly module.</summary>");
        sb.AppendLine("        public static global::InterBus.IInterBusBuilder AddConsumers(");
        sb.AppendLine("            this global::InterBus.IInterBusBuilder builder)");
        sb.AppendLine("        {");

        for (var i = 0; i < consumers.Count; i++)
        {
            var (consumer, _) = consumers[i];
            var consumerFqn = consumer.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            sb.AppendLine($"            var e{i} = _dispatches[typeof({consumerFqn})];");
            sb.AppendLine($"            builder.AddConsumer<{consumerFqn}>(e{i}.MessageTypes, e{i}.Dispatch);");
        }

        sb.AppendLine("            return builder;");
        sb.AppendLine("        }");
        sb.AppendLine();

        // TryGetDispatch(): individual consumer lookup for AddConsumer<TConsumer>().
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// Tries to get the source-generated dispatch for a specific consumer type.");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        public static bool TryGetDispatch(");
        sb.AppendLine("            global::System.Type consumerType,");
        sb.AppendLine("            out global::System.Type[] messageTypes,");
        sb.AppendLine($"            out {DispatchFuncType} dispatch)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (_dispatches.TryGetValue(consumerType, out var entry))");
        sb.AppendLine("            {");
        sb.AppendLine("                messageTypes = entry.MessageTypes;");
        sb.AppendLine("                dispatch = entry.Dispatch;");
        sb.AppendLine("                return true;");
        sb.AppendLine("            }");
        sb.AppendLine("            messageTypes = global::System.Array.Empty<global::System.Type>();");
        sb.AppendLine("            dispatch = null!;");
        sb.AppendLine("            return false;");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
    }

    // ── Aggregating class ────────────────────────────────────────────────────────────────────

    private static void AppendAggregatingClass(StringBuilder sb, List<string> allModuleNames)
    {
        sb.AppendLine("    /// <summary>");
        sb.AppendLine("    /// Source-generated consumer registrations and dispatch delegates.");
        sb.AppendLine("    /// Produced by <c>InterBus.SourceGenerator</c> at compile time -");
        sb.AppendLine("    /// no runtime reflection is required for dispatch.");
        sb.AppendLine("    /// </summary>");
        sb.AppendLine($"    internal static class {AggregatingClassName}");
        sb.AppendLine("    {");

        // AddConsumer<TConsumer>(): individual consumer registration via dispatch module lookup.
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// Register a specific consumer by its type using the source-generated dispatch delegate.");
        sb.AppendLine("        /// Searches all discovered dispatch modules for <typeparamref name=\"TConsumer\"/>.");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"TConsumer\">The consumer type to register. Must be an <c>IConsumer&lt;T&gt;</c>");
        sb.AppendLine("        /// implementation discovered by the source generator in this assembly or a referenced one.</typeparam>");
        sb.AppendLine("        /// <param name=\"builder\">The <see cref=\"global::InterBus.IInterBusBuilder\"/> to register the consumer with.</param>");
        sb.AppendLine("        /// <returns>The same builder instance for fluent chaining.</returns>");
        sb.AppendLine("        /// <exception cref=\"global::System.InvalidOperationException\">Thrown when <typeparamref name=\"TConsumer\"/> was not");
        sb.AppendLine("        /// found in any dispatch module. Ensure the consumer implements <c>IConsumer&lt;T&gt;</c> and");
        sb.AppendLine("        /// <c>InterBus.SourceGenerator</c> is referenced in the assembly that defines it.</exception>");
        sb.AppendLine("        public static global::InterBus.IInterBusBuilder AddConsumer<");
        sb.AppendLine("            [global::System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(");
        sb.AppendLine("                global::System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(");
        sb.AppendLine("            this global::InterBus.IInterBusBuilder builder)");
        sb.AppendLine("            where TConsumer : class, global::InterBus.IConsumer");
        sb.AppendLine("        {");

        for (var i = 0; i < allModuleNames.Count; i++)
        {
            sb.AppendLine($"            if (global::{GeneratedNamespace}.{allModuleNames[i]}.TryGetDispatch(");
            sb.AppendLine($"                    typeof(TConsumer), out var m{i}, out var d{i}))");
            sb.AppendLine($"                return builder.AddConsumer<TConsumer>(m{i}, d{i});");
        }

        sb.AppendLine("            throw new global::System.InvalidOperationException(");
        sb.AppendLine("                $\"No source-generated dispatch found for consumer type '{typeof(TConsumer).FullName}'. \"");
        sb.AppendLine("                + \"Ensure the consumer implements IConsumer<T> and InterBus.SourceGenerator is referenced \"");
        sb.AppendLine("                + \"in the assembly that defines it.\");");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
    }

    // ── Dispatch body emitter ────────────────────────────────────────────────────────────────

    private static void AppendDispatchBody(
        StringBuilder sb,
        INamedTypeSymbol consumer,
        List<INamedTypeSymbol> messages)
    {
        var consumerFqn = consumer.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

        var consumerTypes = string.Join(", ",
            messages.Select(m => $"typeof({m.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)})"));
        
        sb.AppendLine($"            [typeof({consumerFqn})] = (");
        sb.AppendLine($"                new global::System.Type[] {{ {consumerTypes} }},");
        sb.AppendLine("                static (sp, payload, context, ct) =>");
        sb.AppendLine("                {");
        sb.AppendLine("                    if (!context.Headers.TryGetValue(global::InterBus.InterBusHeaders.MessageType, out var msgType))");
        sb.AppendLine("                    {");
        sb.AppendLine("                        sp.GetService<global::Microsoft.Extensions.Logging.ILoggerFactory>()");
        sb.AppendLine($"                            ?.CreateLogger(typeof({consumerFqn}).FullName!)");
        sb.AppendLine("                            .LogError(");
        sb.AppendLine("                                \"InterBus: message received without {{Header}} header for consumer {{Consumer}}\",");
        sb.AppendLine("                                global::InterBus.InterBusHeaders.MessageType,");
        sb.AppendLine($"                                \"{consumer.Name}\");");
        sb.AppendLine("                        return global::System.Threading.Tasks.Task.CompletedTask;");
        sb.AppendLine("                    }");
        sb.AppendLine("                    var serializer = sp.GetRequiredService<global::InterBus.InterBusOptions>().Serializer;");
        sb.AppendLine($"                    var consumer = sp.GetRequiredService<{consumerFqn}>();");

        foreach (var message in messages)
        {
            var messageFqn = message.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var messageFullName = message.ToDisplayString();

            sb.AppendLine($"                    if (msgType == \"{messageFullName}\")");
            sb.AppendLine("                        return consumer.Consume(");
            sb.AppendLine($"                            serializer.Deserialize<{messageFqn}>(payload)!,");
            sb.AppendLine("                            context, ct);");
        }

        sb.AppendLine("                    sp.GetService<global::Microsoft.Extensions.Logging.ILoggerFactory>()");
        sb.AppendLine($"                        ?.CreateLogger(typeof({consumerFqn}).FullName!)");
        sb.AppendLine("                        .LogError(");
        sb.AppendLine("                            \"InterBus: consumer {{Consumer}} does not handle message type {{MessageType}}; message ignored\",");
        sb.AppendLine($"                            \"{consumer.Name}\", msgType);");
        sb.AppendLine("                    return global::System.Threading.Tasks.Task.CompletedTask;");
        sb.AppendLine("                }),");
    }

    // ── Helper: find dispatch modules from referenced assemblies ────────────────────────────

    /// <summary>
    /// Scans only the <c>InterBus.Generated</c> namespace of the merged
    /// <see cref="Compilation.GlobalNamespace"/> for <c>public static</c> types whose name
    /// starts with <see cref="DispatchModulePrefix"/>. These are the per-assembly dispatch
    /// modules emitted by this generator when the referenced assemblies were compiled.
    /// </summary>
    private static List<string> FindExternalDispatchModules(Compilation compilation)
    {
        var modules = new List<string>();

        // Navigate to InterBus.Generated in the merged global namespace.
        var ns = compilation.GlobalNamespace;
        foreach (var part in GeneratedNamespace.Split('.'))
        {
            ns = ns.GetNamespaceMembers().FirstOrDefault(n => n.Name == part);
            if (ns is null) return modules;
        }

        // Collect all public static classes whose name starts with the dispatch module prefix.
        foreach (var type in ns.GetTypeMembers())
        {
            if (type.Name.StartsWith(DispatchModulePrefix, global::System.StringComparison.Ordinal) &&
                type.TypeKind == TypeKind.Class && type is
                    { IsStatic: true, DeclaredAccessibility: Accessibility.Public })
            {
                modules.Add(type.Name);
            }
        }

        return modules;
    }

    // ── Assembly name sanitizer ──────────────────────────────────────────────────────────────

    private static string SanitizeAssemblyName(string name)
    {
        if (name.Length == 0) return "_";
        var sb = new StringBuilder(name.Length);
        foreach (var c in name)
            sb.Append(char.IsLetterOrDigit(c) ? c : '_');
        if (char.IsDigit(sb[0]))
            sb.Insert(0, '_');
        return sb.ToString();
    }
}
