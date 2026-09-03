using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public static class FindHitExtensions
{
    public static string? GetIndex<T>(this FindHit<T> hit)
    {
        return hit?.Data?.GetString(ElasticDataKeys.Index);
    }

    public static object[]? GetSorts<T>(this FindHit<T> hit)
    {
        if (hit is null || !hit.Data.TryGetValue(ElasticDataKeys.Sorts, out object? sorts))
            return Array.Empty<object>();

        // Handle different collection types - new ES client returns IReadOnlyCollection<FieldValue>
        if (sorts is object[] sortsArray)
            return sortsArray;

        if (sorts is IEnumerable<FieldValue> fieldValues)
        {
            // Extract actual values from FieldValue objects
            return fieldValues.Select(GetFieldValueAsObject).ToArray()!;
        }

        if (sorts is IEnumerable<object> sortsList)
            return sortsList.ToArray();

        return Array.Empty<object>();
    }

    private static object? GetFieldValueAsObject(FieldValue fv)
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

    public static string? GetSearchBeforeToken<T>(this FindResults<T> results) where T : class
    {
        if (results == null || results.Hits.Count == 0)
            return null;

        return results.Data.GetString(ElasticDataKeys.SearchBeforeToken, null);
    }

    public static string? GetSearchAfterToken<T>(this FindResults<T> results) where T : class
    {
        if (results == null || results.Hits.Count == 0)
            return null;

        return results.Data.GetString(ElasticDataKeys.SearchAfterToken, null);
    }

    internal static void SetSearchBeforeToken<T>(this FindResults<T> results, ITextSerializer serializer) where T : class
    {
        if (results == null || results.Hits.Count == 0)
            return;

        string? token = results.Hits.First().GetSortToken(serializer);
        if (!String.IsNullOrEmpty(token))
            results.Data[ElasticDataKeys.SearchBeforeToken] = token;
    }

    internal static void SetSearchAfterToken<T>(this FindResults<T> results, ITextSerializer serializer) where T : class
    {
        if (results == null || results.Hits.Count == 0)
            return;

        string? token = results.Hits.Last().GetSortToken(serializer);
        if (!String.IsNullOrEmpty(token))
            results.Data[ElasticDataKeys.SearchAfterToken] = token;
    }

    public static string? GetSortToken<T>(this FindHit<T> hit, ITextSerializer serializer)
    {
        object[]? sorts = hit?.GetSorts();
        if (sorts is null || sorts.Length is 0)
            return null;

        return Encode(serializer.SerializeToString(sorts));
    }

    /// <summary>
    /// Reverses a sort in place, preserving its selected value and reversing missing-value placement.
    /// </summary>
    /// <remarks>
    /// Materializes direction-dependent defaults before reversing so backward cursors use the same
    /// values as forward cursors. See <see href="https://www.elastic.co/docs/reference/elasticsearch/rest-apis/sort-search-results">Elasticsearch sort semantics</see>.
    /// </remarks>
    public static SortOptions? ReverseOrder(this SortOptions? sort)
    {
        if (sort is null)
            return null;

        if (sort.Field is { } field)
        {
            var order = field.Order ?? (field.Field.Name is "_score" ? SortOrder.Desc : SortOrder.Asc);
            if (field.Field.Name is not ("_score" or "_doc" or "_shard_doc"))
            {
                // Reversing direction must keep the same value from multivalued fields and
                // invert missing placement, rather than adopting the opposite order's defaults.
                field.Mode ??= order is SortOrder.Asc ? SortMode.Min : SortMode.Max;
                field.Missing = field.Missing switch
                {
                    null or "_last" => "_first",
                    "_first" => "_last",
                    _ => field.Missing
                };
            }

            field.Order = ReverseSortOrder(order);
        }
        else if (sort.Score is { } score)
        {
            score.Order = ReverseSortOrder(score.Order ?? SortOrder.Desc);
        }
        else if (sort.Doc is { } doc)
        {
            doc.Order = ReverseSortOrder(doc.Order ?? SortOrder.Asc);
        }
        else if (sort.GeoDistance is { } geoDistance)
        {
            var order = geoDistance.Order ?? SortOrder.Asc;
            geoDistance.Mode ??= order is SortOrder.Asc ? SortMode.Min : SortMode.Max;
            geoDistance.Order = ReverseSortOrder(order);
        }
        else if (sort.Script is { } script)
        {
            var order = script.Order ?? SortOrder.Asc;
            script.Mode ??= order is SortOrder.Asc ? SortMode.Min : SortMode.Max;
            script.Order = ReverseSortOrder(order);
        }

        return sort;
    }

    private static SortOrder ReverseSortOrder(SortOrder order) => order is SortOrder.Asc ? SortOrder.Desc : SortOrder.Asc;

    public static IEnumerable<SortOptions>? ReverseOrder(this IEnumerable<SortOptions>? sorts)
    {
        if (sorts == null)
            return null;

        var sortList = sorts.ToList();
        sortList.ForEach(s => s.ReverseOrder());
        return sortList;
    }

    public static object[]? DecodeSortToken(string sortToken, ITextSerializer serializer)
    {
        return serializer.Deserialize<object[]>(Decode(sortToken));
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
    public const string PointInTimeId = "pointintimeid";
    public const string Sorts = "sorts";
    public const string SearchBeforeToken = nameof(SearchBeforeToken);
    public const string SearchAfterToken = nameof(SearchAfterToken);
}
