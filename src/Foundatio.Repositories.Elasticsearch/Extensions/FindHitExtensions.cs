using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Nest;

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

        object[] sortsArray = sorts as object[];
        return sortsArray;
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

    public static ISort ReverseOrder(this ISort sort)
    {
        if (sort == null)
            return null;

        sort.Order = !sort.Order.HasValue || sort.Order == SortOrder.Ascending ? SortOrder.Descending : SortOrder.Ascending;
        return sort;
    }

    public static IEnumerable<ISort> ReverseOrder(this IEnumerable<ISort> sorts)
    {
        if (sorts == null)
            return null;

        var sortList = sorts.ToList();
        sortList.ForEach(s => s.ReverseOrder());
        return sortList;
    }

    public static object[] DecodeSortToken(string sortToken)
    {
        var tokens = JsonSerializer.Deserialize<object[]>(Decode(sortToken), _options);
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
        throw new NotImplementedException();
    }
}
