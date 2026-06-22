using System.Linq.Expressions;

namespace InterBus.AmazonSqs;

internal static class SnsSqsSubscriptionFilterTranslator
{
    public static bool TryBuildMessageBodyFilterPolicy(
        LambdaExpression expression,
        out Dictionary<string, string[]> filterPolicy)
    {
        var entries = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        if (!TryCollect(expression.Body, entries))
        {
            filterPolicy = [];
            return false;
        }

        filterPolicy = entries.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value.ToArray(),
            StringComparer.Ordinal);
        return filterPolicy.Count > 0;
    }

    private static bool TryCollect(
        Expression expression,
        Dictionary<string, List<string>> entries)
    {
        expression = StripConvert(expression);

        return expression switch
        {
            BinaryExpression { NodeType: ExpressionType.AndAlso } andAlso =>
                TryCollect(andAlso.Left, entries) && TryCollect(andAlso.Right, entries),
            BinaryExpression { NodeType: ExpressionType.OrElse } orElse =>
                TryCollectOrElse(orElse, entries),
            BinaryExpression { NodeType: ExpressionType.Equal } equals =>
                TryCollectEquality(equals, entries),
            _ => false
        };
    }

    private static bool TryCollectOrElse(
        BinaryExpression orElse,
        Dictionary<string, List<string>> entries)
    {
        var leftEntries = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var rightEntries = new Dictionary<string, List<string>>(StringComparer.Ordinal);

        if (!TryCollect(orElse.Left, leftEntries) || !TryCollect(orElse.Right, rightEntries))
            return false;

        if (leftEntries.Count != 1 || rightEntries.Count != 1)
            return false;

        var (leftPath, leftValues) = leftEntries.Single();
        var (rightPath, rightValues) = rightEntries.Single();

        if (!string.Equals(leftPath, rightPath, StringComparison.Ordinal))
            return false;

        foreach (var value in leftValues)
            AddEntry(entries, leftPath, value);

        foreach (var value in rightValues)
            AddEntry(entries, rightPath, value);

        return true;
    }

    private static bool TryCollectEquality(
        BinaryExpression equals,
        Dictionary<string, List<string>> entries)
    {
        var left = StripConvert(equals.Left);
        var right = StripConvert(equals.Right);

        if (TryGetMemberPath(left, out var path) && TryGetConstantValue(right, out var value))
            return AddEntry(entries, path, value);

        if (TryGetMemberPath(right, out path) && TryGetConstantValue(left, out value))
            return AddEntry(entries, path, value);

        return false;
    }

    private static bool AddEntry(
        Dictionary<string, List<string>> entries,
        string path,
        string value)
    {
        if (!entries.TryGetValue(path, out var values))
        {
            values = [];
            entries[path] = values;
        }

        if (!values.Contains(value, StringComparer.Ordinal))
            values.Add(value);

        return true;
    }

    private static bool TryGetMemberPath(Expression expression, out string path)
    {
        var parts = new Stack<string>();
        var current = expression;

        while (current is MemberExpression member)
        {
            parts.Push(member.Member.Name);
            current = member.Expression!;
        }

        if (current is not ParameterExpression)
        {
            path = string.Empty;
            return false;
        }

        path = string.Join('.', parts);
        return path.Length > 0;
    }

    private static bool TryGetConstantValue(Expression expression, out string value)
    {
        expression = StripConvert(expression);

        if (expression is ConstantExpression constant)
        {
            value = ConvertToFilterValue(constant.Value);
            return value.Length > 0;
        }

        try
        {
            var lambda = Expression.Lambda(expression);
            var compiled = lambda.Compile();
            value = ConvertToFilterValue(compiled.DynamicInvoke());
            return value.Length > 0;
        }
        catch
        {
            value = string.Empty;
            return false;
        }
    }

    private static string ConvertToFilterValue(object? value) =>
        value switch
        {
            null => string.Empty,
            string s => s,
            bool b => b ? "true" : "false",
            Enum e => e.ToString(),
            Guid g => g.ToString(),
            _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
        };

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            expression = unary.Operand;

        return expression;
    }
}
