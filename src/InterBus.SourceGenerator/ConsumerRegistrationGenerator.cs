using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace InterBus.SourceGenerator;

/// <summary>
/// Incremental source generator that produces compile-time consumer and saga registrations and
/// dispatch delegates for InterBus.
///
/// <para><b>Architecture - decoupled dispatch from registration:</b></para>
/// <list type="bullet">
///   <item><b>Per-assembly dispatch module</b> – emitted in every project that has
///   <c>IConsumer&lt;T&gt;</c> or <c>ISagaConsumer&lt;TState, TMessage&gt;</c> implementations,
///   using <c>SyntaxProvider</c> so the step is fast and incremental (only re-runs when class
///   declarations change in the current project).
///   The generated <c>public static class InterBusConsumerRegistrations_{AssemblyName}</c>
///   contains the dispatch delegates, an <c>AddConsumers()</c> method, a
///   <c>TryGetDispatch()</c> lookup method, and a <c>TryGetSagaDispatch()</c> lookup.</item>
///
///   <item><b>Aggregating registrations class</b> – emitted in every project that can see one
///   or more dispatch modules (either from the current project or from referenced assemblies).
///   This step scans <em>only</em> the <c>InterBus.Generated</c> namespace for
///   <c>InterBusConsumerRegistrations_*</c> types - a targeted, cheap scan rather than walking
///   all types in the global namespace.  The generated <c>internal static class
///   InterBusConsumerRegistrations</c> exposes <c>AddConsumers()</c>,
///   <c>AddConsumer&lt;TConsumer&gt;()</c>, and <c>AddSaga&lt;TSaga, TState&gt;()</c>
///   extension methods that cover all discovered modules.</item>
/// </list>
///
/// <para>This design allows consumers and sagas defined in library assemblies (including NuGet
/// packages) to be registered in a host project without any runtime reflection, while keeping
/// incremental compilation fast.</para>
/// </summary>
[Generator]
public sealed class ConsumerRegistrationGenerator : IIncrementalGenerator
{
    private const string ConsumerInterfaceMetadataName = "InterBus.IConsumer`1";
    private const string SagaConsumerInterfaceMetadataName = "InterBus.Sagas.ISagaConsumer`2";
    private const string GeneratedNamespace = "InterBus.Generated";

    // Prefix for per-assembly dispatch modules; also used to discover them in GlobalNamespace.
    private const string DispatchModulePrefix = "InterBusConsumerRegistrations_";
    private const string AggregatingClassName = "InterBusConsumerRegistrations";

    private const string DispatchFuncType =
        "global::System.Func<global::InterBus.Dispatching.DispatchContext, global::System.IServiceProvider, global::System.Threading.CancellationToken, global::System.Threading.Tasks.Task>";

    private static readonly DiagnosticDescriptor MixedSagaStateTypesDiagnostic = new(
        id: "INTERBUS001",
        title: "Saga has mixed state types",
        messageFormat: "Saga '{0}' implements ISagaConsumer<,> with multiple different state types. " +
                       "All ISagaConsumer<TState, TMessage> implementations on a single saga class must use " +
                       "the same TState. Conflicting types: '{1}' and '{2}'.",
        category: "InterBus.SourceGenerator",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

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
        //   (a) resolving IConsumer<T> / ISagaConsumer<,> interface symbols
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

        var sagaConsumerInterface = compilation.GetTypeByMetadataName(SagaConsumerInterfaceMetadataName);

        // ── Collect IConsumer<T> implementations ────────────────────────────
        var currentConsumers = new List<(INamedTypeSymbol Consumer, List<INamedTypeSymbol> Messages)>();

        // ── Collect ISagaConsumer<TState, TMessage> implementations ─────────
        var currentSagas =
            new List<(INamedTypeSymbol Saga, INamedTypeSymbol StateType, List<INamedTypeSymbol> Messages)>();

        foreach (var classSymbol in syntaxClasses)
        {
            if (classSymbol.IsAbstract || classSymbol.TypeKind != TypeKind.Class) continue;

            List<INamedTypeSymbol>? consumerMessages = null;
            INamedTypeSymbol? sagaStateType = null;
            List<INamedTypeSymbol>? sagaMessages = null;
            INamedTypeSymbol? conflictingStateType = null;

            foreach (var symbol in classSymbol.AllInterfaces)
            {
                // IConsumer<T> check
                if (symbol.TypeArguments.Length == 1 &&
                    SymbolEqualityComparer.Default.Equals(symbol.OriginalDefinition, consumerInterface) &&
                    symbol.TypeArguments[0] is INamedTypeSymbol consumerMsg)
                {
                    consumerMessages ??= new List<INamedTypeSymbol>();
                    consumerMessages.Add(consumerMsg);
                }

                // ISagaConsumer<TState, TMessage> check
                if (sagaConsumerInterface is not null &&
                    symbol.TypeArguments.Length == 2 &&
                    SymbolEqualityComparer.Default.Equals(symbol.OriginalDefinition, sagaConsumerInterface) &&
                    symbol.TypeArguments[0] is INamedTypeSymbol stateType &&
                    symbol.TypeArguments[1] is INamedTypeSymbol sagaMsg)
                {
                    // Validate consistent state type across all ISagaConsumer<> implementations.
                    // Track the first conflicting type so we can include it in the diagnostic.
                    if (sagaStateType is null)
                    {
                        sagaStateType = stateType;
                    }
                    else if (!SymbolEqualityComparer.Default.Equals(sagaStateType, stateType))
                    {
                        conflictingStateType ??= stateType;
                        continue;
                    }

                    sagaMessages ??= new List<INamedTypeSymbol>();
                    sagaMessages.Add(sagaMsg);
                }
            }

            if (consumerMessages is not null)
                currentConsumers.Add((classSymbol, consumerMessages));

            if (conflictingStateType is not null)
            {
                // Report a compile-time error and skip this saga so no broken dispatch is emitted.
                ctx.ReportDiagnostic(Diagnostic.Create(
                    MixedSagaStateTypesDiagnostic,
                    classSymbol.Locations.FirstOrDefault(),
                    classSymbol.Name,
                    sagaStateType!.ToDisplayString(),
                    conflictingStateType.ToDisplayString()));
            }
            else if (sagaStateType is not null && sagaMessages is not null)
            {
                currentSagas.Add((classSymbol, sagaStateType, sagaMessages));
            }
        }

        // Looks only in the InterBus.Generated namespace for InterBusConsumerRegistrations_*
        // These modules were generated by this same generator when the referenced assemblies
        // were compiled.
        var externalModuleNames = FindExternalDispatchModules(compilation);

        // Nothing to generate.
        if (currentConsumers.Count == 0 && currentSagas.Count == 0 && externalModuleNames.Count == 0) return;

        var assemblyName = SanitizeAssemblyName(compilation.AssemblyName ?? "Unknown");
        var currentModuleName = DispatchModulePrefix + assemblyName;

        // AddConsumer<TConsumer>() / AddSaga<TSaga, TState>() must be able to find types from
        // referenced assemblies, so we still build the full module list for those methods.
        var allModuleNames = new List<string>(externalModuleNames);
        if (currentConsumers.Count > 0 || currentSagas.Count > 0)
            allModuleNames.Add(currentModuleName);

        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("using global::Microsoft.Extensions.DependencyInjection;");
        sb.AppendLine();
        sb.AppendLine($"namespace {GeneratedNamespace}");
        sb.AppendLine("{");

        if (currentConsumers.Count > 0 || currentSagas.Count > 0)
        {
            AppendDispatchModule(sb, currentModuleName, currentConsumers, currentSagas);
            sb.AppendLine();
        }

        AppendAggregatingClass(sb, allModuleNames);

        sb.AppendLine("}");

        ctx.AddSource(
            "InterBusConsumerRegistrations.g.cs",
            SourceText.From(sb.ToString(), Encoding.UTF8));
    }

    private static void AppendDispatchModule(
        StringBuilder sb,
        string moduleName,
        List<(INamedTypeSymbol Consumer, List<INamedTypeSymbol> Messages)> consumers,
        List<(INamedTypeSymbol Saga, INamedTypeSymbol StateType, List<INamedTypeSymbol> Messages)> sagas)
    {
        sb.AppendLine("    /// <summary>");
        sb.AppendLine("    /// Source-generated per-assembly dispatch module");
        sb.AppendLine("    /// Produced by <c>InterBus.SourceGenerator</c> for this assembly's");
        sb.AppendLine("    /// <c>IConsumer&lt;T&gt;</c> and <c>ISagaConsumer&lt;TState,TMessage&gt;</c>");
        sb.AppendLine($"    /// implementations. Consumed by the aggregating <c>{AggregatingClassName}</c>");
        sb.AppendLine("    /// class. Not intended for direct use.");
        sb.AppendLine("    /// </summary>");
        sb.AppendLine("    [global::System.ComponentModel.EditorBrowsable(");
        sb.AppendLine("        global::System.ComponentModel.EditorBrowsableState.Never)]");
        sb.AppendLine($"    public static class {moduleName}");
        sb.AppendLine("    {");

        // ── Consumer dispatch table ─────────────────────────────────────────
        sb.AppendLine("        private static readonly global::System.Collections.Generic.Dictionary<");
        sb.AppendLine("            global::System.Type,");
        sb.AppendLine($"            (global::System.Type[] MessageTypes, {DispatchFuncType} Dispatch)>");
        sb.AppendLine("            _dispatches = new()");
        sb.AppendLine("        {");

        foreach (var (consumer, messages) in consumers)
        {
            AppendConsumerDispatchBody(sb, consumer, messages);
        }

        sb.AppendLine("        };");
        sb.AppendLine();

        // ── Saga dispatch table ─────────────────────────────────────────────
        sb.AppendLine("        private static readonly global::System.Collections.Generic.Dictionary<");
        sb.AppendLine("            global::System.Type,");
        sb.AppendLine($"            (global::System.Type[] MessageTypes, {DispatchFuncType} Dispatch)>");
        sb.AppendLine("            _sagaDispatches = new()");
        sb.AppendLine("        {");

        foreach (var (saga, stateType, messages) in sagas)
        {
            AppendSagaDispatchBody(sb, saga, stateType, messages);
        }

        sb.AppendLine("        };");
        sb.AppendLine();

        // ── AddConsumers(): statically typed AddConsumer<T> calls ───────────
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

        // ── TryGetDispatch(): individual consumer lookup ────────────────────
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// Tries to get the source-generated dispatch for a specific consumer type");
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
        sb.AppendLine();

        // ── TryGetSagaDispatch(): individual saga lookup ────────────────────
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// Tries to get the source-generated dispatch for a specific saga type");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        public static bool TryGetSagaDispatch(");
        sb.AppendLine("            global::System.Type sagaType,");
        sb.AppendLine("            out global::System.Type[] messageTypes,");
        sb.AppendLine($"            out {DispatchFuncType} dispatch)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (_sagaDispatches.TryGetValue(sagaType, out var entry))");
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

    private static void AppendAggregatingClass(StringBuilder sb, List<string> allModuleNames)
    {
        sb.AppendLine("    /// <summary>");
        sb.AppendLine("    /// Source-generated consumer and saga registrations and dispatch delegates");
        sb.AppendLine("    /// Produced by <c>InterBus.SourceGenerator</c> at compile time -");
        sb.AppendLine("    /// no runtime reflection is required for dispatch");
        sb.AppendLine("    /// </summary>");
        sb.AppendLine($"    internal static class {AggregatingClassName}");
        sb.AppendLine("    {");

        // ── AddConsumer<TConsumer>(configure) ───────────────────────────────
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// Register a specific consumer by its type using the source-generated dispatch delegate.");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"TConsumer\">The consumer type to register. Must be an <c>IConsumer&lt;T&gt;</c>");
        sb.AppendLine("        /// implementation discovered by the source generator in this assembly or a referenced one.</typeparam>");
        sb.AppendLine("        /// <param name=\"builder\">The <see cref=\"global::InterBus.IInterBusBuilder\"/> to register the consumer with</param>");
        sb.AppendLine("        /// <param name=\"configure\">The <paramref name=\"configure\"/> delegate is invoked immediately after registration, allowing per-consumer options such as concurrent dispatch to be set inline</param>");
        sb.AppendLine("        /// <returns>The same builder instance for fluent chaining</returns>");
        sb.AppendLine("        /// <exception cref=\"global::System.InvalidOperationException\">Thrown when <typeparamref name=\"TConsumer\"/> was not");
        sb.AppendLine("        /// found in any dispatch module. Ensure the consumer implements <c>IConsumer&lt;T&gt;</c> and");
        sb.AppendLine("        /// <c>InterBus.SourceGenerator</c> is referenced in the assembly that defines it.</exception>");
        sb.AppendLine("        public static global::InterBus.IInterBusBuilder AddConsumer<");
        sb.AppendLine("            [global::System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(");
        sb.AppendLine("                global::System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(");
        sb.AppendLine("            this global::InterBus.IInterBusBuilder builder,");
        sb.AppendLine("            global::System.Action<global::InterBus.IConsumerBuilder>? configure = null)");
        sb.AppendLine("            where TConsumer : class, global::InterBus.IConsumer");
        sb.AppendLine("        {");

        for (var i = 0; i < allModuleNames.Count; i++)
        {
            sb.AppendLine($"            if (global::{GeneratedNamespace}.{allModuleNames[i]}.TryGetDispatch(");
            sb.AppendLine($"                    typeof(TConsumer), out var m{i}, out var d{i}))");
            sb.AppendLine($"                return builder.AddConsumer<TConsumer>(m{i}, d{i}, configure);");
        }

        sb.AppendLine("            throw new global::System.InvalidOperationException(");
        sb.AppendLine("                $\"No source-generated dispatch found for consumer type '{typeof(TConsumer).FullName}'. \"");
        sb.AppendLine("                + \"Ensure the consumer implements IConsumer<T> and InterBus.SourceGenerator is referenced \"");
        sb.AppendLine("                + \"in the assembly that defines it.\");");
        sb.AppendLine("        }");
        sb.AppendLine();

        // ── AddSaga<TSaga, TState>(configure) ───────────────────────────────
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// Register a specific saga by its type using the source-generated dispatch delegate.");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <remarks>");
        sb.AppendLine("        /// The <paramref name=\"configure\"/> delegate must configure the saga repository");
        sb.AppendLine("        /// (e.g. <c>saga.UseInMemoryRepository()</c>) otherwise a runtime DI resolution");
        sb.AppendLine("        /// failure will occur when the first message arrives.");
        sb.AppendLine("        /// </remarks>");
        sb.AppendLine("        public static global::InterBus.IInterBusBuilder AddSaga<");
        sb.AppendLine("            [global::System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(");
        sb.AppendLine("                global::System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors)] TSaga,");
        sb.AppendLine("            TState>(");
        sb.AppendLine("            this global::InterBus.IInterBusBuilder builder,");
        sb.AppendLine("            global::System.Action<global::InterBus.Sagas.ISagaBuilder<TState>>? configure = null)");
        sb.AppendLine("            where TSaga : class, global::InterBus.Sagas.ISaga<TState>");
        sb.AppendLine("            where TState : class, new()");
        sb.AppendLine("        {");

        for (var i = 0; i < allModuleNames.Count; i++)
        {
            sb.AppendLine($"            if (global::{GeneratedNamespace}.{allModuleNames[i]}.TryGetSagaDispatch(");
            sb.AppendLine($"                    typeof(TSaga), out var ms{i}, out var ds{i}))");
            sb.AppendLine($"                return builder.AddSaga<TSaga, TState>(ms{i}, ds{i}, configure);");
        }

        sb.AppendLine("            throw new global::System.InvalidOperationException(");
        sb.AppendLine("                $\"No source-generated dispatch found for saga type '{typeof(TSaga).FullName}'. \"");
        sb.AppendLine("                + \"Ensure the saga implements ISagaConsumer<TState, TMessage> and InterBus.SourceGenerator is referenced \"");
        sb.AppendLine("                + \"in the assembly that defines it.\");");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
    }

    private static void AppendConsumerDispatchBody(
        StringBuilder sb,
        INamedTypeSymbol consumer,
        List<INamedTypeSymbol> messages)
    {
        var consumerFqn = consumer.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

        var consumerTypes = string.Join(", ",
            messages.Select(m => $"typeof({m.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)})"));

        sb.AppendLine($"            [typeof({consumerFqn})] = (");
        sb.AppendLine($"                new global::System.Type[] {{ {consumerTypes} }},");
        sb.AppendLine("                static (context, sp, ct) =>");
        sb.AppendLine("                {");
        sb.AppendLine("                    if (!context.Headers.TryGetValue(global::InterBus.InterBusHeaders.MessageType, out var msgType))");
        sb.AppendLine("                        throw new global::System.InvalidOperationException(");
        sb.AppendLine($"                            $\"InterBus: message received without {{(global::InterBus.InterBusHeaders.MessageType)}} header for consumer {consumer.Name}\");");
        sb.AppendLine("                    var serializer = sp.GetRequiredService<global::InterBus.IMessageSerializer>();");
        sb.AppendLine("                    var consumePipeline = sp.GetRequiredService<global::InterBus.Consuming.ConsumePipelineExecutor>();");
        sb.AppendLine($"                    var consumer = sp.GetRequiredService<{consumerFqn}>();");

        foreach (var message in messages)
        {
            var messageFqn = message.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var messageFullName = message.ToDisplayString();

            sb.AppendLine($"                    if (msgType == \"{messageFullName}\")");
            sb.AppendLine("                    {");
            sb.AppendLine($"                        var message = serializer.Deserialize<{messageFqn}>(context.Payload)!;");
            sb.AppendLine($"                        var consumeContext = new global::InterBus.Consuming.ConsumeContext<{messageFqn}>(context.Descriptor, message, context.Headers);");
            sb.AppendLine("                        return consumePipeline.ExecuteAsync(");
            sb.AppendLine("                            consumeContext,");
            sb.AppendLine("                            consumer.Consume,");
            sb.AppendLine("                            ct);");
            sb.AppendLine("                    }");
        }

        sb.AppendLine("                    throw new global::System.InvalidOperationException(");
        sb.AppendLine($"                        $\"InterBus: consumer {consumer.Name} does not handle message type {{msgType}}\");");
        sb.AppendLine("                }),");
    }

    private static void AppendSagaDispatchBody(
        StringBuilder sb,
        INamedTypeSymbol saga,
        INamedTypeSymbol stateType,
        List<INamedTypeSymbol> messages)
    {
        var sagaFqn = saga.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var stateFqn = stateType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

        var messageTypesList = string.Join(", ",
            messages.Select(m => $"typeof({m.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)})"));

        sb.AppendLine($"            [typeof({sagaFqn})] = (");
        sb.AppendLine($"                new global::System.Type[] {{ {messageTypesList} }},");
        sb.AppendLine("                static (context, sp, ct) =>");
        sb.AppendLine("                {");
        sb.AppendLine("                    if (!context.Headers.TryGetValue(global::InterBus.InterBusHeaders.MessageType, out var msgType))");
        sb.AppendLine("                        throw new global::System.InvalidOperationException(");
        sb.AppendLine($"                            $\"InterBus: message received without {{(global::InterBus.InterBusHeaders.MessageType)}} header for saga {saga.Name}\");");
        sb.AppendLine("                    var serializer = sp.GetRequiredService<global::InterBus.IMessageSerializer>();");
        sb.AppendLine("                    var consumePipeline = sp.GetRequiredService<global::InterBus.Consuming.ConsumePipelineExecutor>();");
        sb.AppendLine($"                    var saga = sp.GetRequiredService<{sagaFqn}>();");
        sb.AppendLine($"                    var sagaExecutor = sp.GetRequiredService<global::InterBus.Sagas.SagaExecutor<{sagaFqn}, {stateFqn}>>();");

        foreach (var message in messages)
        {
            var messageFqn = message.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var messageFullName = message.ToDisplayString();

            sb.AppendLine($"                    if (msgType == \"{messageFullName}\")");
            sb.AppendLine("                    {");
            sb.AppendLine($"                        var message = serializer.Deserialize<{messageFqn}>(context.Payload)!;");
            sb.AppendLine($"                        var consumeContext = new global::InterBus.Consuming.ConsumeContext<{messageFqn}>(context.Descriptor, message, context.Headers);");
            sb.AppendLine("                        return consumePipeline.ExecuteAsync(");
            sb.AppendLine("                            consumeContext,");
            sb.AppendLine($"                            (msg, ctx, innerCt) => sagaExecutor.ExecuteAsync<{messageFqn}>(saga, msg, ctx, innerCt),");
            sb.AppendLine("                            ct);");
            sb.AppendLine("                    }");
        }

        sb.AppendLine("                    throw new global::System.InvalidOperationException(");
        sb.AppendLine($"                        $\"InterBus: saga {saga.Name} does not handle message type {{msgType}}\");");
        sb.AppendLine("                }),");
    }

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
            if (type.Name.StartsWith(DispatchModulePrefix, System.StringComparison.Ordinal) &&
                type.TypeKind == TypeKind.Class && type is
                    { IsStatic: true, DeclaredAccessibility: Accessibility.Public })
            {
                modules.Add(type.Name);
            }
        }

        return modules;
    }

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
