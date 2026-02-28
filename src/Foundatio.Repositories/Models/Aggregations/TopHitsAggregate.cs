using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Foundatio.Repositories.Models;

public class TopHitsAggregate : MetricAggregateBase
{
    private readonly IList<ILazyDocument> _hits;

    public long Total { get; set; }
    public double? MaxScore { get; set; }

    /// <summary>
    /// Raw JSON sources for each hit, used for serialization/deserialization round-tripping (e.g., caching).
    /// </summary>
    public IList<string> Hits { get; set; }

    public TopHitsAggregate(IList<ILazyDocument> hits)
    {
        _hits = hits ?? new List<ILazyDocument>();
    }

    public TopHitsAggregate() { }

    public IReadOnlyCollection<T> Documents<T>() where T : class
    {
        if (_hits != null && _hits.Count > 0)
            return _hits.Select(h => h.As<T>()).ToList();

        if (Hits != null && Hits.Count > 0)
        {
            return Hits
                .Select(json =>
                {
                    if (string.IsNullOrEmpty(json))
                        return null;
                    var lazy = new LazyDocument(Encoding.UTF8.GetBytes(json));
                    return lazy.As<T>();
                })
                .Where(d => d != null)
                .ToList();
        }

        return new List<T>();
    }
}
