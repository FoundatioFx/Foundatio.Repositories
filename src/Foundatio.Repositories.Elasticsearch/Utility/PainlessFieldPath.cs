using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace Foundatio.Repositories.Elasticsearch.Utility;

internal static partial class PainlessFieldPath
{
    [GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]*$")]
    private static partial Regex IdentifierRegex();

    private static readonly FrozenSet<string> s_painlessReservedWords = new HashSet<string>(StringComparer.Ordinal)
    {
        "if", "else", "while", "do", "for", "in", "continue", "break", "return", "new",
        "try", "catch", "throw", "this", "instanceof", "boolean", "byte", "short", "char",
        "int", "long", "float", "double", "void", "def", "null", "true", "false"
    }.ToFrozenSet();

    internal static bool IsIdentifier(string segment) =>
        IdentifierRegex().IsMatch(segment) && !s_painlessReservedWords.Contains(segment);

    internal static string AppendSegment(string prefix, string segment)
    {
        return IsIdentifier(segment)
            ? $"{prefix}.{segment}"
            : $"{prefix}['{segment}']";
    }

    internal static void Validate(string fieldPath, [CallerArgumentExpression(nameof(fieldPath))] string paramName = "")
    {
        ArgumentException.ThrowIfNullOrEmpty(fieldPath, paramName);

        if (fieldPath.StartsWith('.') || fieldPath.EndsWith('.') || fieldPath.Contains(".."))
            throw new ArgumentException($"Field path '{fieldPath}' contains empty segments.", paramName);

        if (fieldPath.Contains('\'') || fieldPath.Contains('\\'))
            throw new ArgumentException($"Field path '{fieldPath}' contains characters that cannot be used in Painless string literals.", paramName);
    }
}
