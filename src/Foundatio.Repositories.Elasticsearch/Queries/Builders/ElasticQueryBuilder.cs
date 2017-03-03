using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Extensions;

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

        public void UseQueryParser(ElasticQueryParser parser) {
            Unregister<ExpressionQueryBuilder>();
            Register(new ParsedExpressionQueryBuilder(parser));

            Unregister<AggregationsQueryBuilder>();
            Register(new AggregationsQueryBuilder());
        }

        public void UseAliases(AliasMap aliasMap) {
            Unregister<ExpressionQueryBuilder>();
            Register(new AliasedExpressionQueryBuilder(aliasMap));
        }

        public void RegisterDefaults() {
            Register<PagableQueryBuilder>();
            Register<FieldIncludesQueryBuilder>();
            Register<SortQueryBuilder>();
            Register(new AggregationsQueryBuilder());
            Register(new ParentQueryBuilder(this));
            Register(new ChildQueryBuilder(this));
            Register<IdentityQueryBuilder>();
            Register<SoftDeletesQueryBuilder>();
            Register<DateRangeQueryBuilder>();
            Register(new ExpressionQueryBuilder());
            Register<ElasticFilterQueryBuilder>();
            Register<FieldConditionsQueryBuilder>();
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            foreach (var builder in _partBuilders)
                await builder.BuildAsync(ctx).AnyContext();
        }

        private static readonly Lazy<ElasticQueryBuilder> _default = new Lazy<ElasticQueryBuilder>(() => new ElasticQueryBuilder());
        public static ElasticQueryBuilder Default => _default.Value;
    }
}