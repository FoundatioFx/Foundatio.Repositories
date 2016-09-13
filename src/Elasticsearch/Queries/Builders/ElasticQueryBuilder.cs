using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Repositories.Queries.Builders;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ElasticQueryBuilder : IElasticQueryBuilder {
        private readonly List<IElasticQueryBuilder> _partBuilders = new List<IElasticQueryBuilder>();

        public ElasticQueryBuilder(bool registerDefaultBuilders = true) {
            if (registerDefaultBuilders)
                RegisterDefaults();
        }

        public void Register<T>(bool replace = true) where T : IElasticQueryBuilder, new() {
            Register(new T(), replace);
        }

        public void Register(params IElasticQueryBuilder[] builders) {
            foreach (var builder in builders)
                Register(builder);
        }

        public void Register<T>(T builder, bool replace = true) where T : IElasticQueryBuilder {
            if (replace) {
                int existing = _partBuilders.FindIndex(b => b.GetType() == typeof(T));
                if (existing >= 0)
                    _partBuilders.RemoveAt(existing);
            }

            _partBuilders.Add(builder);
        }

        public bool Unregister<T>() where T : IElasticQueryBuilder {
            int existing = _partBuilders.FindIndex(b => b.GetType() == typeof(T));
            if (existing < 0)
                return false;

            _partBuilders.RemoveAt(existing);

            return true;
        }

        public void UseMacros(Action<ElasticQueryParserConfiguration> configure) {
            Unregister<SearchQueryBuilder>();
            Register(new ElasticMacroSearchQueryBuilder(new ElasticQueryParser(configure)));
        }

        public void UseAliases(AliasMap aliasMap) {
            Unregister<SearchQueryBuilder>();
            Register(new AliasedSearchQueryBuilder(aliasMap));
        }

        public void RegisterDefaults() {
            Register<PagableQueryBuilder>();
            Register<SelectedFieldsQueryBuilder>();
            Register<SortableQueryBuilder>();
            Register<AggregationsQueryBuilder>();
            Register(new ParentQueryBuilder(this));
            Register(new ChildQueryBuilder(this));
            Register<IdentityQueryBuilder>();
            Register<SoftDeletesQueryBuilder>();
            Register<DateRangeQueryBuilder>();
            Register(new SearchQueryBuilder());
            Register(new SystemFilterQueryBuilder(this));
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