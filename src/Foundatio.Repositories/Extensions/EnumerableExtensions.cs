using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Extensions;

public static class EnumerableExtensions
{
    public static void EnsureIds<T>(this IEnumerable<T> values, Func<T, string> generateIdFunc = null, TimeProvider timeProvider = null) where T : class, IIdentity
    {
        if (values == null)
            return;

        timeProvider ??= TimeProvider.System;

        generateIdFunc ??= _ => ObjectId.GenerateNewId(timeProvider.GetUtcNow().UtcDateTime).ToString();

        foreach (var value in values)
        {
            if (String.IsNullOrEmpty(value.Id))
                value.Id = generateIdFunc(value);
        }
    }

    public static void SetDates<T>(this IEnumerable<T> values, TimeProvider timeProvider = null) where T : class, IHaveDates
    {
        if (values == null)
            return;

        timeProvider ??= TimeProvider.System;

        foreach (var value in values)
        {
            var utcNow = timeProvider.GetUtcNow().UtcDateTime;
            if (value.CreatedUtc == DateTime.MinValue || value.CreatedUtc > utcNow)
                value.CreatedUtc = utcNow;

            value.UpdatedUtc = utcNow;
        }
    }

    public static void SetCreatedDates<T>(this IEnumerable<T> values, TimeProvider timeProvider = null) where T : class, IHaveCreatedDate
    {
        if (values == null)
            return;

        timeProvider ??= TimeProvider.System;

        foreach (var value in values)
        {
            if (value.CreatedUtc == DateTime.MinValue || value.CreatedUtc > timeProvider.GetUtcNow().UtcDateTime)
                value.CreatedUtc = timeProvider.GetUtcNow().UtcDateTime;
        }
    }

    public static void AddRange<T>(this ICollection<T> list, IEnumerable<T> range)
    {
        foreach (var r in range)
            list.Add(r);
    }

    public static IList<TR> FullOuterGroupJoin<TA, TB, TK, TR>(
        this IEnumerable<TA> a,
        IEnumerable<TB> b,
        Func<TA, TK> selectKeyA,
        Func<TB, TK> selectKeyB,
        Func<IEnumerable<TA>, IEnumerable<TB>, TK, TR> projection,
        IEqualityComparer<TK> cmp = null)
    {
        cmp = cmp ?? EqualityComparer<TK>.Default;
        var alookup = a.ToLookup(selectKeyA, cmp);
        var blookup = b.ToLookup(selectKeyB, cmp);

        var keys = new HashSet<TK>(alookup.Select(p => p.Key), cmp);
        keys.UnionWith(blookup.Select(p => p.Key));

        var join = from key in keys
                   let xa = alookup[key]
                   let xb = blookup[key]
                   select projection(xa, xb, key);

        return join.ToList();
    }

    public static IList<TR> FullOuterJoin<TA, TB, TK, TR>(
        this IEnumerable<TA> a,
        IEnumerable<TB> b,
        Func<TA, TK> selectKeyA,
        Func<TB, TK> selectKeyB,
        Func<TA, TB, TK, TR> projection,
        TA defaultA = default,
        TB defaultB = default,
        IEqualityComparer<TK> cmp = null)
    {
        cmp = cmp ?? EqualityComparer<TK>.Default;
        var alookup = a.ToLookup(selectKeyA, cmp);
        var blookup = (b ?? new List<TB>()).ToLookup(selectKeyB, cmp);

        var keys = new HashSet<TK>(alookup.Select(p => p.Key), cmp);
        keys.UnionWith(blookup.Select(p => p.Key));

        var join = from key in keys
                   from xa in alookup[key].DefaultIfEmpty(defaultA)
                   from xb in blookup[key].DefaultIfEmpty(defaultB)
                   select projection(xa, xb, key);

        return join.ToList();
    }

    /// <summary>
    /// Combines both the modified (current) and original (previous) values from a collection of modified documents.
    /// Useful for cache invalidation when you need keys from both the old and new state of changed documents.
    /// </summary>
    public static IReadOnlyCollection<T> UnionOriginalAndModified<T>(this IReadOnlyCollection<ModifiedDocument<T>> documents) where T : class, new()
    {
        return documents.Select(d => d.Value)
            .Union(documents.Select(d => d.Original).Where(d => d is not null))
            .ToList();
    }

    /// <summary>
    /// Converts a byte array to Hexadecimal.
    /// </summary>
    /// <param name="bytes">The bytes to convert.</param>
    /// <returns>Hexadecimal string of the byte array.</returns>
    public static string ToHex(this IEnumerable<byte> bytes)
    {
        var sb = new StringBuilder();
        foreach (byte b in bytes)
            sb.Append(b.ToString("x2"));
        return sb.ToString();
    }
}
