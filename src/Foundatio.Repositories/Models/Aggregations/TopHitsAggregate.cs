using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Models;

public class TopHitsAggregate : MetricAggregateBase
{
    private readonly IList<ILazyDocument> _hits = [];

    public long Total { get; set; }
    public double? MaxScore { get; set; }

    /// <summary>
    /// Raw JSON sources for each hit, used for serialization/deserialization round-tripping (e.g., caching).
    /// </summary>
    public IReadOnlyList<string> Hits { get; set; } = [];

    public TopHitsAggregate(IList<ILazyDocument> hits)
    {
        _hits = hits ?? [];
    }

    public TopHitsAggregate() { }

    /// <param name="serializer">
    /// Required when this aggregate was round-tripped through the cache (i.e., <see cref="Hits"/> is populated).
    /// Can be omitted when reading directly from an Elasticsearch response where <c>_hits</c> are populated from
    /// the live response.
    /// </param>
    public IReadOnlyCollection<T> Documents<T>(ITextSerializer? serializer = null) where T : class
    {
        if (_hits.Count > 0)
            return _hits.Select(h => h.As<T>()).OfType<T>().ToList().AsReadOnly();

        if (Hits is { Count: > 0 })
        {
            ArgumentNullException.ThrowIfNull(serializer);

            return Hits
                .Select(json =>
                {
                    if (String.IsNullOrEmpty(json))
                        return null;
                    var lazy = new LazyDocument(Encoding.UTF8.GetBytes(json), serializer);
                    return lazy.As<T>();
                })
                .OfType<T>()
                .ToList()
                .AsReadOnly();
        }

        return new List<T>();
    }
}
