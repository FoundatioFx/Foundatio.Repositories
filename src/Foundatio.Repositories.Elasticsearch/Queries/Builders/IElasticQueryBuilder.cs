using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Extensions;
using Nest;
using Foundatio.Repositories.Options;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IElasticQueryBuilder {
        Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new();
    }

    public class QueryBuilderContext<T> : IQueryBuilderContext, IElasticQueryVisitorContext, IQueryVisitorContextWithFieldResolver, IQueryVisitorContextWithIncludeResolver, IQueryVisitorContextWithValidation where T : class, new() {
        public QueryBuilderContext(IRepositoryQuery source, ICommandOptions options, SearchDescriptor<T> search = null, IQueryBuilderContext parentContext = null) {
            Source = source;
            Options = options;
            Search = search ?? new SearchDescriptor<T>();
            Parent = parentContext;
            ((IQueryVisitorContextWithIncludeResolver)this).IncludeResolver = options.GetIncludeResolver();
            ((IQueryVisitorContextWithFieldResolver)this).FieldResolver = options.GetQueryFieldResolver();
            ((IElasticQueryVisitorContext)this).MappingResolver = options.GetMappingResolver();

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
        public QueryValidationOptions ValidationOptions { get; set; }
        public QueryValidationInfo ValidationInfo { get; set; }
        QueryFieldResolver IQueryVisitorContextWithFieldResolver.FieldResolver { get; set; }
        IncludeResolver IQueryVisitorContextWithIncludeResolver.IncludeResolver { get; set; }
        ElasticMappingResolver IElasticQueryVisitorContext.MappingResolver { get; set; }
        ICollection<ElasticRuntimeField> IElasticQueryVisitorContext.RuntimeFields { get; } = new List<ElasticRuntimeField>();
        RuntimeFieldResolver IElasticQueryVisitorContext.RuntimeFieldResolver { get; set; }

        GroupOperator IQueryVisitorContext.DefaultOperator { get; set; }
        Lazy<string> IElasticQueryVisitorContext.DefaultTimeZone { get; set; }
        bool IElasticQueryVisitorContext.UseScoring { get; set; }
        string[] IQueryVisitorContext.DefaultFields { get; set; }
        string IQueryVisitorContext.QueryType { get; set; }

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
        public static void SetTimeZone(this IQueryBuilderContext context, Lazy<string> timeZone) {
            if (context is not IElasticQueryVisitorContext elasticContext)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            elasticContext.DefaultTimeZone = timeZone;
        }

        public static void SetTimeZone(this IQueryBuilderContext context, string timeZone) {
            if (context is not IElasticQueryVisitorContext elasticContext)
                throw new ArgumentException("Context must be of type IElasticQueryVisitorContext", nameof(context));

            elasticContext.DefaultTimeZone = new Lazy<string>(() => timeZone);
        }

        public static string GetTimeZone(this IQueryBuilderContext context) {
            var elasticContext = context as IElasticQueryVisitorContext;
            return elasticContext?.DefaultTimeZone?.Value;
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