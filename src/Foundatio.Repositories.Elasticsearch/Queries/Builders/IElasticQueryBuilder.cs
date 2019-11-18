using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Extensions;
using Nest;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IElasticQueryBuilder {
        Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new();
    }

    public class QueryBuilderContext<T> : IQueryBuilderContext, IElasticQueryVisitorContext, IQueryVisitorContextWithFieldResolver, IQueryVisitorContextWithIncludeResolver where T : class, new() {
        public QueryBuilderContext(IRepositoryQuery source, ICommandOptions options, SearchDescriptor<T> search = null, IQueryBuilderContext parentContext = null) {
            Source = source;
            Options = options;
            Search = search ?? new SearchDescriptor<T>();
            Parent = parentContext;
            ((IQueryVisitorContextWithIncludeResolver)this).IncludeResolver = options.GetIncludeResolver();
            ((IQueryVisitorContextWithFieldResolver)this).FieldResolver = options.GetQueryFieldResolver();

            var range = GetDateRange();
            if (range != null) {
                Data.Add(nameof(range.StartDate), range.GetStartDate());
                Data.Add(nameof(range.EndDate), range.GetEndDate());
            }
        }

        public IQueryBuilderContext Parent { get; }
        public IRepositoryQuery Source { get; }
        public ICommandOptions Options { get; }
        public QueryContainer Query { get; set; }
        public QueryContainer Filter { get; set; }
        public SearchDescriptor<T> Search { get; }
        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();

        QueryFieldResolver IQueryVisitorContextWithFieldResolver.FieldResolver { get; set; }
        IncludeResolver IQueryVisitorContextWithIncludeResolver.IncludeResolver { get; set; }

        Operator IElasticQueryVisitorContext.DefaultOperator { get; set; }
        string IElasticQueryVisitorContext.DefaultTimeZone { get; set; }
        bool IElasticQueryVisitorContext.UseScoring { get; set; }
        string[] IQueryVisitorContext.DefaultFields { get; set; }
        string IQueryVisitorContext.QueryType { get; set; }
        Func<string, IProperty> IElasticQueryVisitorContext.GetPropertyMappingFunc { get; set; }
        IDictionary<string, object> IQueryVisitorContext.Data { get; } = new Dictionary<string, object>();

        private DateRange GetDateRange() {
            foreach (var dateRange in Source.GetDateRanges()) {
                if (dateRange.UseDateRange)
                    return dateRange;
            }

            return null;
        }
    }

    public interface IQueryBuilderContext {
        IQueryBuilderContext Parent { get; }
        IRepositoryQuery Source { get; }
        ICommandOptions Options { get; }
        QueryContainer Query { get; set; }
        QueryContainer Filter { get; set; }
        IDictionary<string, object> Data { get; }
    }

    public static class QueryBuilderContextExtensions {
        public static void SetTimeZone(this IQueryBuilderContext context, string timeZone) {
            var elasticContext = context as IElasticQueryVisitorContext;
            if (elasticContext == null)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            elasticContext.DefaultTimeZone = timeZone;
        }

        public static string GetTimeZone(this IQueryBuilderContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            return elasticContext?.DefaultTimeZone;
        }
    }

    public static class ElasticQueryBuilderExtensions {
        public static async Task<QueryContainer> BuildQueryAsync<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, ICommandOptions options, SearchDescriptor<T> search) where T : class, new() {
            var ctx = new QueryBuilderContext<T>(query, options, search);
            await builder.BuildAsync(ctx).AnyContext();

            return new BoolQuery {
                Must = new[] { ctx.Query },
                Filter = new[] { ctx.Filter ?? new MatchAllQuery() }
            };
        }

        public static async Task ConfigureSearchAsync<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, ICommandOptions options, SearchDescriptor<T> search) where T : class, new() {
            if (search == null)
                throw new ArgumentNullException(nameof(search));

            var q = await builder.BuildQueryAsync(query, options, search).AnyContext();
            search.Query(d => q);
        }
    }
}