using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Foundatio.Repositories.Elasticsearch.Utility;

public ref struct FieldIncludeParser
{
    private readonly ReadOnlySpan<char> _source;
    private int _position;
    private readonly FieldIncludeParseResult _result = new();
    private readonly Stack<FieldInclude> _includeStack = new();
    private FieldInclude _current = null;

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
                _includeStack.Push(_current);
                Advance();
            }
            else if (Current == ')')
            {
                if (_includeStack.Count == 0)
                    return new FieldIncludeParseResult { IsValid = false, ValidationMessage = "Found unexpected ')' character" };

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
                    if (existing != null)
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

        if (_includeStack.Count > 0)
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

    public static FieldIncludeParseResult Parse(string expression)
    {
        return Parse(expression.AsSpan());
    }

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

    public IList<string> ToFieldPaths()
    {
        var fields = new List<string>();

        foreach (var field in Fields)
            field.ToFlattenedFields(fields, String.Empty);

        return fields;
    }
}

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
