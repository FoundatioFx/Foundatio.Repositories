using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders;

public interface IElasticQueryBuilder
{
    Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new();
}

public class QueryBuilderContext<T> : IQueryBuilderContext, IElasticQueryVisitorContext, IQueryVisitorContextWithFieldResolver, IQueryVisitorContextWithIncludeResolver, IQueryVisitorContextWithValidation where T : class, new()
{
    public QueryBuilderContext(IRepositoryQuery source, ICommandOptions options, SearchRequestDescriptor<T> search = null, IQueryBuilderContext parentContext = null)
    {
        Source = source;
        Options = options;
        Search = search ?? new SearchRequestDescriptor<T>();
        Parent = parentContext;
        ((IQueryVisitorContextWithIncludeResolver)this).IncludeResolver = options.GetIncludeResolver();
        ((IQueryVisitorContextWithFieldResolver)this).FieldResolver = options.GetQueryFieldResolver();
        ((IElasticQueryVisitorContext)this).MappingResolver = options.GetMappingResolver();

        var range = GetDateRange();
        if (range != null)
        {
            Data.Add(nameof(range.StartDate), range.GetStartDate());
            Data.Add(nameof(range.EndDate), range.GetEndDate());
        }
    }

    public IQueryBuilderContext Parent { get; }
    public IRepositoryQuery Source { get; }
    public ICommandOptions Options { get; }
    public Query Query { get; set; }
    public Query Filter { get; set; }
    public SearchRequestDescriptor<T> Search { get; }
    public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();
    public QueryValidationOptions ValidationOptions { get; set; }
    public QueryValidationResult ValidationResult { get; set; }
    QueryFieldResolver IQueryVisitorContextWithFieldResolver.FieldResolver { get; set; }
    IncludeResolver IQueryVisitorContextWithIncludeResolver.IncludeResolver { get; set; }
    ElasticMappingResolver IElasticQueryVisitorContext.MappingResolver { get; set; }
    ICollection<ElasticRuntimeField> IElasticQueryVisitorContext.RuntimeFields { get; } = new List<ElasticRuntimeField>();
    bool? IElasticQueryVisitorContext.EnableRuntimeFieldResolver { get; set; }
    RuntimeFieldResolver IElasticQueryVisitorContext.RuntimeFieldResolver { get; set; }

    GroupOperator IQueryVisitorContext.DefaultOperator { get; set; }
    Func<Task<string>> IElasticQueryVisitorContext.DefaultTimeZone { get; set; }
    bool IElasticQueryVisitorContext.UseScoring { get; set; }
    string[] IQueryVisitorContext.DefaultFields { get; set; }
    string IQueryVisitorContext.QueryType { get; set; }

    private DateRange GetDateRange()
    {
        foreach (var dateRange in Source.GetDateRanges())
        {
            if (dateRange.UseDateRange)
                return dateRange;
        }

        return null;
    }
}

public interface IQueryBuilderContext
{
    IQueryBuilderContext Parent { get; }
    IRepositoryQuery Source { get; }
    ICommandOptions Options { get; }
    Query Query { get; set; }
    Query Filter { get; set; }
    IDictionary<string, object> Data { get; }
}

public static class QueryBuilderContextExtensions
{
    public static void SetTimeZone(this IQueryBuilderContext context, Func<Task<string>> timeZone)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        elasticContext.DefaultTimeZone = timeZone;
    }

    public static void SetTimeZone(this IQueryBuilderContext context, string timeZone)
    {
        if (context is not IElasticQueryVisitorContext elasticContext)
            throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

        elasticContext.DefaultTimeZone = () => Task.FromResult(timeZone);
    }

    public static Task<string> GetTimeZoneAsync(this IQueryBuilderContext context)
    {
        var elasticContext = context as IElasticQueryVisitorContext;
        if (elasticContext?.DefaultTimeZone != null)
            return elasticContext.DefaultTimeZone.Invoke();

        return Task.FromResult<string>(null);
    }
}

public static class ElasticQueryBuilderExtensions
{
    public static async Task<Query> BuildQueryAsync<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, ICommandOptions options, SearchRequestDescriptor<T> search) where T : class, new()
    {
        var ctx = new QueryBuilderContext<T>(query, options, search);
        await builder.BuildAsync(ctx).AnyContext();

        return new BoolQuery
        {
            Must = new[] { ctx.Query },
            Filter = new[] { ctx.Filter ?? new MatchAllQuery() }
        };
    }

    public static async Task ConfigureSearchAsync<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, ICommandOptions options, SearchRequestDescriptor<T> search) where T : class, new()
    {
        if (search == null)
            throw new ArgumentNullException(nameof(search));

        var q = await builder.BuildQueryAsync(query, options, search).AnyContext();
        search.Query(d => q);
    }
}
