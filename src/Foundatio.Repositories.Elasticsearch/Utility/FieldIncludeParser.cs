using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Foundatio.Repositories.Elasticsearch.Utility;

/// <summary>
/// Allocation-free parser for Google FieldMask-style expressions that specify nested field paths.
/// </summary>
/// <remarks>
/// <para>Supported syntax:</para>
/// <list type="bullet">
///   <item>Simple fields: <c>"name"</c> produces <c>["name"]</c></item>
///   <item>Comma-separated: <c>"name,age"</c> produces <c>["name", "age"]</c></item>
///   <item>Dotted paths: <c>"address.street"</c> produces <c>["address.street"]</c></item>
///   <item>Nested groups: <c>"address(street,city)"</c> produces <c>["address.street", "address.city"]</c></item>
///   <item>Deep nesting: <c>"results(id,program(name,id))"</c> produces <c>["results.id", "results.program.name", "results.program.id"]</c></item>
/// </list>
/// <para>
/// Duplicate field names at the same nesting level are merged so their sub-fields combine.
/// Use <see cref="ParseFieldPaths(string)"/> for the common case of getting flattened dotted path strings.
/// </para>
/// </remarks>
public ref struct FieldIncludeParser
{
    private readonly ReadOnlySpan<char> _source;
    private int _position;
    private readonly FieldIncludeParseResult _result = new();
    private readonly Stack<FieldInclude> _includeStack = new();
    private FieldInclude _current = null;
    private int _openParenCount = 0;

    public FieldIncludeParser(in ReadOnlySpan<char> source)
    {
        _source = source;
        _position = 0;
    }

    public FieldIncludeParseResult Parse()
    {
        while (!IsEOF)
        {
            if (Current == ',')
            {
                Advance();
            }
            else if (Current == '(')
            {
                _openParenCount++;
                if (_current is not null)
                    _includeStack.Push(_current);

                Advance();
            }
            else if (Current == ')')
            {
                if (_openParenCount == 0)
                    return new FieldIncludeParseResult { IsValid = false, ValidationMessage = "Found unexpected ')' character" };

                _openParenCount--;
                if (_includeStack.Count > 0)
                    _includeStack.Pop();

                Advance();
            }
            else
            {
                var fieldName = ReadName();
                if (fieldName.Length > 0)
                {
                    var fieldList = _includeStack.Count > 0 ? _includeStack.Peek().SubFields : _result.Fields;
                    var fieldNameString = fieldName.ToString().Trim();
                    if (String.IsNullOrWhiteSpace(fieldNameString))
                        continue;

                    var existing = fieldList.FirstOrDefault(f => String.Equals(f.Name, fieldNameString, StringComparison.OrdinalIgnoreCase));
                    if (existing is not null)
                    {
                        _current = existing;
                    }
                    else
                    {
                        _current = new FieldInclude(fieldNameString);
                        fieldList.Add(_current);
                    }
                }
            }
        }

        if (_openParenCount > 0)
            return new FieldIncludeParseResult { IsValid = false, ValidationMessage = "Missing end of group ')' character" };

        return _result;
    }

    private char Current => _source[_position];
    private bool IsEOF => _position == _source.Length;

    private ReadOnlySpan<char> ReadName()
    {
        int start = _position;
        ReadUntil(c => c == ',' || c == '/' || c == '(' || c == ')');
        var name = _source.Slice(start, _position - start);

        return name;
    }

    private bool Advance()
    {
        if (_position == _source.Length)
            return false;

        _position++;
        return true;
    }

    private void ReadUntil(Func<char, bool> condition)
    {
        while (!IsEOF && !condition(Current))
            if (!Advance())
                return;
    }

    /// <summary>
    /// Parses a field mask expression string and returns the structured result.
    /// </summary>
    public static FieldIncludeParseResult Parse(string expression)
    {
        return Parse(expression.AsSpan());
    }

    /// <summary>
    /// Parses a field mask expression and returns the flattened dotted field paths.
    /// For example, <c>"results(id,program(name))"</c> returns <c>["results.id", "results.program.name"]</c>.
    /// </summary>
    public static IList<string> ParseFieldPaths(string expression)
    {
        return ParseFieldPaths(expression.AsSpan());
    }

    public static FieldIncludeParseResult Parse(in ReadOnlySpan<char> expression)
    {
        var parser = new FieldIncludeParser(in expression);
        return parser.Parse();
    }

    public static IList<string> ParseFieldPaths(in ReadOnlySpan<char> expression)
    {
        var parser = new FieldIncludeParser(in expression);
        var result = parser.Parse();

        return result.ToFieldPaths();
    }
}

/// <summary>
/// The result of parsing a field mask expression via <see cref="FieldIncludeParser"/>.
/// Contains the hierarchical list of parsed fields and a validity flag.
/// Use <see cref="ToFieldPaths"/> to flatten the result into dotted path strings.
/// </summary>
public class FieldIncludeParseResult
{
    public IList<FieldInclude> Fields { get; set; } = new List<FieldInclude>();
    public bool IsValid { get; set; } = true;
    public string ValidationMessage { get; set; }

    public override string ToString()
    {
        if (!IsValid)
            return ValidationMessage;

        if (Fields.Count == 0)
            return String.Empty;

        var sb = new StringBuilder();
        for (int i = 0; i < Fields.Count; i++)
        {
            Fields[i].ToStringInternal(sb);
            if (i < Fields.Count - 1)
                sb.Append(',');
        }

        return sb.ToString();
    }

    /// <summary>
    /// Flattens the hierarchical field structure into a list of dotted path strings
    /// (e.g., <c>["results.id", "results.program.name"]</c>).
    /// </summary>
    public IList<string> ToFieldPaths()
    {
        var fields = new List<string>();

        foreach (var field in Fields)
            field.ToFlattenedFields(fields, String.Empty);

        return fields;
    }
}

/// <summary>
/// Represents a single field in a parsed field mask expression. A field has a <see cref="Name"/>
/// and an optional list of <see cref="SubFields"/> representing nested fields grouped by parentheses.
/// </summary>
public class FieldInclude
{
    public FieldInclude(string name)
    {
        Name = name;
    }

    public string Name { get; }
    public IList<FieldInclude> SubFields { get; set; } = new List<FieldInclude>();

    public override string ToString()
    {
        if (SubFields.Count == 0)
            return Name;

        var sb = new StringBuilder();
        ToStringInternal(sb);

        return sb.ToString();
    }

    internal void ToStringInternal(StringBuilder sb)
    {
        sb.Append(Name);

        if (SubFields.Count == 0)
            return;

        sb.Append('(');

        for (int i = 0; i < SubFields.Count; i++)
        {
            SubFields[i].ToStringInternal(sb);
            if (i < SubFields.Count - 1)
                sb.Append(',');
        }

        sb.Append(')');
    }

    internal void ToFlattenedFields(List<string> fields, string path)
    {
        string fieldPath = path.Length > 0 ? path + "." + Name : Name;
        if (SubFields.Count == 0)
        {
            fields.Add(fieldPath);
        }
        else
        {
            foreach (var field in SubFields)
                field.ToFlattenedFields(fields, fieldPath);
        }
    }
}
