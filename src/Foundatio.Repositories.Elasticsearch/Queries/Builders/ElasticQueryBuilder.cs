using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

public class ElasticQueryBuilder : IElasticQueryBuilder {
    private readonly List<ElasticQueryBuilderRegistration> _registrations = new();
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

    public void Replace<TOldQueryBuilder, TNewQueryBuilder>() where TOldQueryBuilder : IElasticQueryBuilder where TNewQueryBuilder : IElasticQueryBuilder, new() {
        int priority = Unregister<TOldQueryBuilder>();
        if (priority == -1)
            priority = 0;
        Register(new TNewQueryBuilder(), false, priority);
    }

    public void RegisterBefore<T>(IElasticQueryBuilder builder, bool replace = true) {
        int priority = 0;
        var referenceBuilder = _registrations.FirstOrDefault(v => typeof(T) == v.Builder.GetType());
        if (referenceBuilder != null)
            priority = referenceBuilder.Priority - 1;
        
        Register(builder, replace, priority);
    }

    public void RegisterBefore<TTarget, TQueryBuilder>(bool replace = true) where TTarget : IElasticQueryBuilder where TQueryBuilder : IElasticQueryBuilder, new() {
        RegisterBefore<TTarget>(new TQueryBuilder(), replace);
    }

    public void RegisterAfter<T>(IElasticQueryBuilder builder, bool replace = true) {
        int priority = 0;
        var referenceBuilder = _registrations.FirstOrDefault(v => typeof(T) == v.Builder.GetType());
        if (referenceBuilder != null)
            priority = referenceBuilder.Priority + 1;

        Register(builder, replace, priority);
    }

    public void RegisterAfter<TTarget, TQueryBuilder>(bool replace = true) where TTarget : IElasticQueryBuilder where TQueryBuilder : IElasticQueryBuilder, new() {
        RegisterAfter<TTarget>(new TQueryBuilder(), replace);
    }

    public int Unregister<T>() where T : IElasticQueryBuilder {
        if (_queryBuilders != null)
            throw new InvalidOperationException("Can not modify query builder registrations after first use.");

        int existing = _registrations.FindIndex(b => b.Builder.GetType() == typeof(T));
        if (existing < 0)
            return -1;

        _registrations.RemoveAt(existing);

        return existing;
    }

    public int GetPriority<T>() where T : IElasticQueryBuilder {
        return _registrations.FindIndex(b => b.Builder.GetType() == typeof(T));
    }

    public void UseQueryParser(ElasticQueryParser parser) {
        Unregister<ExpressionQueryBuilder>();
        Register(new ParsedExpressionQueryBuilder(parser));
        RegisterAfter<ParsedExpressionQueryBuilder, AggregationsQueryBuilder>();
    }

    public void UseAliases(QueryFieldResolver aliasMap) {
        Unregister<ExpressionQueryBuilder>();
        Register(new FieldResolverQueryBuilder(aliasMap));
    }

    public void RegisterDefaults() {
        Register<AddRuntimeFieldsToContextQueryBuilder>();
        Register<PageableQueryBuilder>();
        Register<FieldIncludesQueryBuilder>();
        Register<SortQueryBuilder>();
        Register<AggregationsQueryBuilder>();
        Register<ParentQueryBuilder>();
        Register<ChildQueryBuilder>();
        Register<IdentityQueryBuilder>();
        Register<SoftDeletesQueryBuilder>();
        Register<DateRangeQueryBuilder>();
        Register<ExpressionQueryBuilder>();
        Register<ElasticFilterQueryBuilder>();
        Register<FieldConditionsQueryBuilder>();
        Register<RuntimeFieldsQueryBuilder>(Int32.MaxValue);
        Register<SearchAfterQueryBuilder>(Int32.MaxValue);
    }

    public ElasticQueryBuilderRegistration[] GetRegistrations() {
        return _registrations.OrderBy(v => v.Priority).ToArray();
    }

    public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
        if (_queryBuilders == null)
            Interlocked.CompareExchange(ref _queryBuilders, _registrations.OrderBy(v => v.Priority).Select(r => r.Builder).ToArray(), null);
        
        foreach (var builder in _queryBuilders)
            await builder.BuildAsync(ctx).AnyContext();
    }

    private static readonly Lazy<ElasticQueryBuilder> _default = new(() => new ElasticQueryBuilder());
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

    public override string ToString() {
        return $"Priority: {Priority} Type: {Builder.GetType().Name}";
    }
}
