using System;
using System.Collections.Generic;
using System.Linq;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ElasticQueryBuilder : IElasticQueryBuilder {
        private readonly List<IElasticQueryBuilder> _partBuilders = new List<IElasticQueryBuilder>();

        public ElasticQueryBuilder(bool registerDefaultBuilders = true) {
            if (registerDefaultBuilders)
                RegisterDefaults();
        }

        public void Register<T>() where T : IElasticQueryBuilder, new() {
            if (_partBuilders.Any(b => b.GetType() == typeof(T)))
                return;

             _partBuilders.Add(new T());
        }

        public void Register<T>(Func<IElasticQueryBuilder> creator) where T : IElasticQueryBuilder {
            if (_partBuilders.Any(b => b.GetType() == typeof(T)))
                return;

            _partBuilders.Add(creator());
        }

        public void Register(IElasticQueryBuilder builder) {
            _partBuilders.Add(builder);
        }

        public void Register(params IElasticQueryBuilder[] builders) {
            _partBuilders.AddRange(builders);
        }

        public void RegisterDefaults() {
            Register<PagableQueryBuilder>();
            Register<PagableQueryBuilder>();
            Register<SelectedFieldsQueryBuilder>();
            Register<SortableQueryBuilder>();
            Register<AggregationsQueryBuilder>();
            Register<ParentQueryBuilder>(() => new ParentQueryBuilder(this));
            Register<ChildQueryBuilder>(() => new ChildQueryBuilder(this));
            Register<IdentityQueryBuilder>();
            Register<SoftDeletesQueryBuilder>();
            Register<DateRangeQueryBuilder>();
            Register<SearchQueryBuilder>();
            Register<ElasticFilterQueryBuilder>();
            Register<FieldConditionsQueryBuilder>();
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            foreach (var builder in _partBuilders)
                builder.Build(ctx);
        }

        private static readonly Lazy<ElasticQueryBuilder> _default = new Lazy<ElasticQueryBuilder>(() => new ElasticQueryBuilder());
        public static ElasticQueryBuilder Default => _default.Value;
    }
}