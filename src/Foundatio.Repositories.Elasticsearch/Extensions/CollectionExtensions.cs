using System;
using System.Collections.Generic;
using System.Linq;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class CollectionExtensions { 
    public static IEnumerable<TSource> DistinctBy<TSource, TKey> (this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
    {
        var knownKeys = new HashSet<TKey>();
        foreach (TSource element in source)
        {
            if (knownKeys.Add(keySelector(element)))
            {
                yield return element;
            }
        }
    }

    public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int size) {
        T[] bucket = null;
        int count = 0;

        foreach (var item in source) {
            if (bucket == null)
                bucket = new T[size];

            bucket[count++] = item;

            if (count != size)
                continue;

            yield return bucket.Select(x => x);

            bucket = null;
            count = 0;
        }

        // Return the last bucket with all remaining elements
        if (bucket != null && count > 0) {
            Array.Resize(ref bucket, count);
            yield return bucket.Select(x => x);
        }
    }
}
