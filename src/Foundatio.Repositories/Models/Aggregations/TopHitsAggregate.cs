using System.Collections.Generic;
using System.Linq;
using Foundatio.Serializer;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Models {
	public class TopHitsAggregate : MetricAggregateBase {
		private readonly IList<LazyDocument> _hits;
        
		public long Total { get; set; }
		public double? MaxScore { get; set; }

		public TopHitsAggregate(IList<LazyDocument> hits) {
			_hits = hits;
		}

        public IReadOnlyCollection<T> Documents<T>() where T : class {
            return _hits.Select(h => h.As<T>()).ToList();
        }
	}
}