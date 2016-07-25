using System;
using System.Threading;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class Index : IndexBase {
        public Index(IElasticClient client, string name, ILoggerFactory loggerFactory = null) : base(client, name, loggerFactory) {}

        public override void Configure() {
            IIndicesOperationResponse response = null;

            if (!_client.IndexExists(Name).Exists)
                response = _client.CreateIndex(Name, ConfigureDescriptor);

            _logger.Trace(() => response.GetRequest());
            if (response != null && !response.IsValid)
                throw new ApplicationException("An error occurred creating the index or template: " + response.ServerError.Error);

            while (!_client.IndexExists(Name).Exists)
                SystemClock.Sleep(100);
        }

        public virtual CreateIndexDescriptor ConfigureDescriptor(CreateIndexDescriptor idx) {
            idx.AddAlias(Name);

            foreach (var t in IndexTypes)
                t.Configure(idx);

            return idx;
        }
    }

    public class Index<T> : Index where T: class {
        public Index(IElasticClient client, string name = null, ILoggerFactory loggerFactory = null): base(client, name ?? typeof(T).Name.ToLower(), loggerFactory) {
            Type = new IndexType<T>(this);
            AddType(Type);
        }

        public IndexType<T> Type { get; }
    }
}