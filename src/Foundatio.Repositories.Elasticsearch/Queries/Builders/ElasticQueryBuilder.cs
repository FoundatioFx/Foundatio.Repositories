using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ElasticQueryBuilder : IElasticQueryBuilder {
        private readonly List<ElasticQueryBuilderRegistration> _registrations = new List<ElasticQueryBuilderRegistration>();
        private IElasticQueryBuilder[] _queryBuilders = null;


        public ElasticQueryBuilder(bool registerDefaultBuilders = true) {
            if (registerDefaultBuilders)
                RegisterDefaults();
        }

        public void Register<T>(bool replace = true) where T : IElasticQueryBuilder, new() {
            Register(new T(), replace);
        }

        public void Register<T>(int priority, bool replace = true) where T : IElasticQueryBuilder, new() {
            Register(new T(), replace, priority);
        }

        public void Register(params IElasticQueryBuilder[] builders) {
            foreach (var builder in builders)
                Register(new ElasticQueryBuilderRegistration(builder));
        }

        public void Register(params ElasticQueryBuilderRegistration[] builders) {
            foreach (var builder in builders)
                Register(builder);
        }

        public void Register(ElasticQueryBuilderRegistration registration) {
            if (_queryBuilders != null)
                throw new InvalidOperationException("Can not modify query builder registrations after first use.");
            
            _registrations.Add(registration);
        }

        public void Register(IElasticQueryBuilder builder, int priority) {
            Register(new ElasticQueryBuilderRegistration(builder, priority));
        }

        public void Register<T>(T builder, bool replace = true, int? priority = null) where T : IElasticQueryBuilder {
            if (_queryBuilders != null)
                throw new InvalidOperationException("Can not modify query builder registrations after first use.");
            
            if (replace) {
                int existing = _registrations.FindIndex(b => b.Builder.GetType() == typeof(T));
                if (existing >= 0) {
                    if (priority == null)
                        priority = _registrations[existing].Priority;
                    _registrations.RemoveAt(existing);
                }
            }

            Register(new ElasticQueryBuilderRegistration(builder, priority));
        }

        public void RegisterBefore<T>(IElasticQueryBuilder builder) {
            int priority = 0;
            var referenceBuilder = _registrations.FirstOrDefault(v => typeof(T) == v.Builder.GetType());
            if (referenceBuilder != null)
                priority = referenceBuilder.Priority - 1;
            
            Register(new ElasticQueryBuilderRegistration(builder, priority));
        }

        public void RegisterAfter<T>(IElasticQueryBuilder builder) {
            int priority = 0;
            var referenceBuilder = _registrations.FirstOrDefault(v => typeof(T) == v.Builder.GetType());
            if (referenceBuilder != null)
                priority = referenceBuilder.Priority + 1;
            
            Register(new ElasticQueryBuilderRegistration(builder, priority));
        }

        public bool Unregister<T>() where T : IElasticQueryBuilder {
            if (_queryBuilders != null)
                throw new InvalidOperationException("Can not modify query builder registrations after first use.");

            int existing = _registrations.FindIndex(b => b.Builder.GetType() == typeof(T));
            if (existing < 0)
                return false;

            _registrations.RemoveAt(existing);

            return true;
        }

        public void UseQueryParser(ElasticQueryParser parser) {
            Unregister<ExpressionQueryBuilder>();
            Register(new ParsedExpressionQueryBuilder(parser));

            Unregister<AggregationsQueryBuilder>();
            Register(new AggregationsQueryBuilder());
        }

        public void UseAliases(QueryFieldResolver aliasMap) {
            Unregister<ExpressionQueryBuilder>();
            Register(new FieldResolverQueryBuilder(aliasMap));
        }

        public void RegisterDefaults() {
            Register<PageableQueryBuilder>();
            Register<FieldIncludesQueryBuilder>();
            Register<RuntimeFieldsQueryBuilder>();
            Register<SortQueryBuilder>();
            Register(new AggregationsQueryBuilder());
            Register(new ParentQueryBuilder());
            Register(new ChildQueryBuilder());
            Register<IdentityQueryBuilder>();
            Register<SoftDeletesQueryBuilder>();
            Register<DateRangeQueryBuilder>();
            Register(new ExpressionQueryBuilder());
            Register<ElasticFilterQueryBuilder>();
            Register<FieldConditionsQueryBuilder>();
            Register<SearchAfterQueryBuilder>(Int32.MaxValue);
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            if (_queryBuilders == null)
                Interlocked.CompareExchange(ref _queryBuilders, _registrations.OrderBy(v => v.Priority).Select(r => r.Builder).ToArray(), null);
            
            foreach (var builder in _queryBuilders)
                await builder.BuildAsync(ctx).AnyContext();
        }

        private static readonly Lazy<ElasticQueryBuilder> _default = new Lazy<ElasticQueryBuilder>(() => new ElasticQueryBuilder());
        public static ElasticQueryBuilder Default => _default.Value;
    }
    
    public class ElasticQueryBuilderRegistration {
        private static int _currentAutoPriority;

        public ElasticQueryBuilderRegistration(IElasticQueryBuilder builder, int? priority = null) {
            Builder = builder;
            Priority = priority ?? Interlocked.Increment(ref _currentAutoPriority);
        }
        
        public IElasticQueryBuilder Builder { get; }
        public int Priority { get; }
    }
}