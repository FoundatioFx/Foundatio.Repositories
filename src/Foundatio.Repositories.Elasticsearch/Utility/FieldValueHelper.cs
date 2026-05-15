using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch;

namespace Foundatio.Repositories.Elasticsearch.Utility;

public static class FieldValueHelper
{
    private static readonly ConcurrentDictionary<Type, Dictionary<string, string>> _enumTypeLookup = new();

    public static FieldValue ToFieldValue(object? value)
    {
        return value switch
        {
            null => FieldValue.Null,
            string s => FieldValue.String(s),
            bool b => FieldValue.Boolean(b),
            long l => FieldValue.Long(l),
            int i => FieldValue.Long(i),
            short s16 => FieldValue.Long(s16),
            byte b8 => FieldValue.Long(b8),
            sbyte sb => FieldValue.Long(sb),
            uint ui => FieldValue.Long(ui),
            ulong ul => ul <= (ulong)long.MaxValue ? FieldValue.Long((long)ul) : FieldValue.Double((double)ul),
            ushort us => FieldValue.Long(us),
            double d => FieldValue.Double(d),
            float f => FieldValue.Double(f),
            decimal m => FieldValue.Double((double)m),
            DateTime dt => FieldValue.String(dt.ToString("o")),
            DateTimeOffset dto => FieldValue.String(dto.ToString("o")),
            Enum e => FieldValue.String(GetEnumStringValue(e)),
            _ => FieldValue.String(value.ToString()!)
        };
    }

    private static string GetEnumStringValue(Enum value)
    {
        var lookup = _enumTypeLookup.GetOrAdd(value.GetType(), static type =>
        {
            var map = new Dictionary<string, string>();
            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Static))
            {
                var jsonAttr = field.GetCustomAttribute<JsonStringEnumMemberNameAttribute>();
                if (jsonAttr is not null)
                {
                    map[field.Name] = jsonAttr.Name;
                    continue;
                }

                var enumMemberAttr = field.GetCustomAttribute<EnumMemberAttribute>();
                if (enumMemberAttr?.Value is not null)
                    map[field.Name] = enumMemberAttr.Value;
            }

            return map;
        });

        var name = value.ToString();
        return lookup.TryGetValue(name, out var resolved) ? resolved : name;
    }
}
