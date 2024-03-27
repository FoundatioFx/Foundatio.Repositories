using System.Collections.Generic;
using System.Linq;

namespace Foundatio.Repositories.Models;

public class TopHitsAggregate : MetricAggregateBase
{
    private readonly IList<ILazyDocument> _hits;

    public long Total { get; set; }
    public double? MaxScore { get; set; }

    public TopHitsAggregate(IList<ILazyDocument> hits)
    {
        _hits = hits ?? new List<ILazyDocument>();
    }

    public IReadOnlyCollection<T> Documents<T>() where T : class
    {
        return _hits.Select(h => h.As<T>()).ToList();
    }
}
