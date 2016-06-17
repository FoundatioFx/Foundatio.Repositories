using System;
using System.Collections.Generic;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ElasticQueryBuilder : IQueryBuilder {
        private readonly List<IQueryBuilder> _partBuilders = new List<IQueryBuilder>();

        public ElasticQueryBuilder(bool registerDefaultBuilders = true) {
            if (registerDefaultBuilders)
                RegisterDefaults();
        }

        public void Register(IQueryBuilder builder) {
            _partBuilders.Add(builder);
        }

        public void RegisterDefaults() {
            _partBuilders.Add(new PagableQueryBuilder());
            _partBuilders.Add(new SelectedFieldsQueryBuilder());
            _partBuilders.Add(new SortableQueryBuilder());
            _partBuilders.Add(new FacetQueryBuilder());
            _partBuilders.Add(new ParentQueryBuilder(this));
            _partBuilders.Add(new ChildQueryBuilder(this));
            _partBuilders.Add(new IdentityQueryBuilder());
            _partBuilders.Add(new SoftDeletesQueryBuilder());
            _partBuilders.Add(new DateRangeQueryBuilder());
            _partBuilders.Add(new SearchQueryBuilder());
            _partBuilders.Add(new ElasticFilterQueryBuilder());
            _partBuilders.Add(new FieldConditionsQueryBuilder());
        }

        public void BuildQuery<T>(object query, object options, ref QueryContainer container) where T : class, new() {
            FilterContainer filter = null;
            BuildFilter<T>(query, options, ref filter);

            container &= new FilteredQuery { Filter = filter};

            foreach (var partBuilder in _partBuilders)
                partBuilder.BuildQuery<T>(query, options, ref container);
        }

        public void BuildFilter<T>(object query, object options, ref FilterContainer container) where T : class, new() {
            foreach (var partBuilder in _partBuilders)
                partBuilder.BuildFilter<T>(query, options, ref container);
        }

        public void BuildSearch<T>(object query, object options, ref SearchDescriptor<T> descriptor) where T : class, new() {
            foreach (var partBuilder in _partBuilders)
                partBuilder.BuildSearch(query, options, ref descriptor);
        }
    }
}