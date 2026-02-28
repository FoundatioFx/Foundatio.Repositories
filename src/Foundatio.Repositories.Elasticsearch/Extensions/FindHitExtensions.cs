using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public static class FindHitExtensions
{
    private static JsonSerializerOptions _options;
    static FindHitExtensions()
    {
        _options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        _options.Converters.Add(new ObjectConverter());
    }

    public static string GetIndex<T>(this FindHit<T> hit)
    {
        return hit?.Data?.GetString(ElasticDataKeys.Index);
    }

    public static object[] GetSorts<T>(this FindHit<T> hit)
    {
        if (hit == null || !hit.Data.TryGetValue(ElasticDataKeys.Sorts, out object sorts))
            return Array.Empty<object>();

        // Handle different collection types - new ES client returns IReadOnlyCollection<FieldValue>
        if (sorts is object[] sortsArray)
            return sortsArray;

        if (sorts is IEnumerable<FieldValue> fieldValues)
        {
            // Extract actual values from FieldValue objects
            return fieldValues.Select(fv => GetFieldValueAsObject(fv)).ToArray();
        }

        if (sorts is IEnumerable<object> sortsList)
            return sortsList.ToArray();

        return Array.Empty<object>();
    }

    private static object GetFieldValueAsObject(FieldValue fv)
    {
        // FieldValue is a tagged union in the new ES client
        // We need to extract the actual value based on the variant
        if (fv.TryGetLong(out var longVal))
            return longVal;
        if (fv.TryGetDouble(out var doubleVal))
            return doubleVal;
        if (fv.TryGetString(out var strVal))
            return strVal;
        if (fv.TryGetBool(out var boolVal))
            return boolVal;
        if (fv.IsNull)
            return null;

        // Fallback - return the FieldValue itself
        return fv;
    }

    public static string GetSearchBeforeToken<T>(this FindResults<T> results) where T : class
    {
        if (results == null || results.Hits.Count == 0)
            return null;

        return results.Data.GetString(ElasticDataKeys.SearchBeforeToken, null);
    }

    public static string GetSearchAfterToken<T>(this FindResults<T> results) where T : class
    {
        if (results == null || results.Hits.Count == 0)
            return null;

        return results.Data.GetString(ElasticDataKeys.SearchAfterToken, null);
    }

    internal static void SetSearchBeforeToken<T>(this FindResults<T> results) where T : class
    {
        if (results == null || results.Hits.Count == 0)
            return;

        string token = results.Hits.First().GetSortToken();
        if (!String.IsNullOrEmpty(token))
            results.Data[ElasticDataKeys.SearchBeforeToken] = token;
    }

    internal static void SetSearchAfterToken<T>(this FindResults<T> results) where T : class
    {
        if (results == null || results.Hits.Count == 0)
            return;

        string token = results.Hits.Last().GetSortToken();
        if (!String.IsNullOrEmpty(token))
            results.Data[ElasticDataKeys.SearchAfterToken] = token;
    }

    public static string GetSortToken<T>(this FindHit<T> hit)
    {
        object[] sorts = hit?.GetSorts();
        if (sorts == null || sorts.Length == 0)
            return null;

        return Encode(JsonSerializer.Serialize(sorts));
    }

    public static SortOptions ReverseOrder(this SortOptions sort)
    {
        if (sort == null)
            return null;

        // SortOptions is a discriminated union - we need to reverse the order on the underlying variant
        if (sort.Field != null)
        {
            sort.Field.Order = !sort.Field.Order.HasValue || sort.Field.Order == SortOrder.Asc ? SortOrder.Desc : SortOrder.Asc;
        }
        else if (sort.Score != null)
        {
            sort.Score.Order = !sort.Score.Order.HasValue || sort.Score.Order == SortOrder.Asc ? SortOrder.Desc : SortOrder.Asc;
        }
        else if (sort.Doc != null)
        {
            sort.Doc.Order = !sort.Doc.Order.HasValue || sort.Doc.Order == SortOrder.Asc ? SortOrder.Desc : SortOrder.Asc;
        }
        else if (sort.GeoDistance != null)
        {
            sort.GeoDistance.Order = !sort.GeoDistance.Order.HasValue || sort.GeoDistance.Order == SortOrder.Asc ? SortOrder.Desc : SortOrder.Asc;
        }
        else if (sort.Script != null)
        {
            sort.Script.Order = !sort.Script.Order.HasValue || sort.Script.Order == SortOrder.Asc ? SortOrder.Desc : SortOrder.Asc;
        }

        return sort;
    }

    public static IEnumerable<SortOptions> ReverseOrder(this IEnumerable<SortOptions> sorts)
    {
        if (sorts == null)
            return null;

        var sortList = sorts.ToList();
        sortList.ForEach(s => s.ReverseOrder());
        return sortList;
    }

    public static object[] DecodeSortToken(string sortToken)
    {
        object[] tokens = JsonSerializer.Deserialize<object[]>(Decode(sortToken), _options);
        return tokens;
    }

    private static string Encode(string text)
    {
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(text))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

    private static string Decode(string text)
    {
        text = text.Replace('_', '/').Replace('-', '+');

        switch (text.Length % 4)
        {
            case 2:
                text += "==";
                break;
            case 3:
                text += "=";
                break;
        }

        return Encoding.UTF8.GetString(Convert.FromBase64String(text));
    }
}

public static class ElasticDataKeys
{
    public const string Index = "index";
    public const string ScrollId = "scrollid";
    public const string Sorts = "sorts";
    public const string SearchBeforeToken = nameof(SearchBeforeToken);
    public const string SearchAfterToken = nameof(SearchAfterToken);
}

public class ObjectConverter : JsonConverter<object>
{
    public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return reader.TokenType switch
        {
            JsonTokenType.Number => GetNumber(reader),
            JsonTokenType.String => reader.GetString(),
            JsonTokenType.True => reader.GetBoolean(),
            JsonTokenType.False => reader.GetBoolean(),
            _ => null
        };
    }

    private object GetNumber(Utf8JsonReader reader)
    {
        if (reader.TryGetInt64(out var l))
            return l;
        else if (reader.TryGetDecimal(out var d))
            return d;
        else
            throw new InvalidOperationException("Value is not a number");
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case long l:
                writer.WriteNumberValue(l);
                break;
            case int i:
                writer.WriteNumberValue(i);
                break;
            case double d:
                writer.WriteNumberValue(d);
                break;
            case decimal dec:
                writer.WriteNumberValue(dec);
                break;
            case string s:
                writer.WriteStringValue(s);
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            default:
                JsonSerializer.Serialize(writer, value, value.GetType(), options);
                break;
        }
    }
}
