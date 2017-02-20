using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Options;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Queries;
using Nest;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IElasticQueryBuilder {
        Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new();
    }

    public class QueryBuilderContext<T> : IQueryBuilderContext, IElasticQueryVisitorContext, IQueryVisitorContextWithAliasResolver, IQueryVisitorContextWithIncludeResolver where T : class, new() {
        public QueryBuilderContext(IRepositoryQuery source, ICommandOptions options, SearchDescriptor<T> search = null, IQueryBuilderContext parentContext = null, string type = null) {
            Source = source;
            Options = options;
            Search = search ?? new SearchDescriptor<T>();
            Parent = parentContext;
            Type = type ?? ContextType.Default;
            var elasticQueryOptions = options as IElasticCommandOptions;
            if (elasticQueryOptions != null)
                ((IQueryVisitorContextWithAliasResolver)this).RootAliasResolver = elasticQueryOptions.RootAliasResolver;

            var range = GetDateRange();
            if (range != null) {
                Data.Add(nameof(range.StartDate), range.GetStartDate());
                Data.Add(nameof(range.EndDate), range.GetEndDate());
            }
        }

        public string Type { get; }
        public IQueryBuilderContext Parent { get; }
        public IRepositoryQuery Source { get; }
        public ICommandOptions Options { get; }
        public QueryContainer Query { get; set; }
        public QueryContainer Filter { get; set; }
        public SearchDescriptor<T> Search { get; }
        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();

        AliasResolver IQueryVisitorContextWithAliasResolver.RootAliasResolver { get; set; }
        Func<string, Task<string>> IQueryVisitorContextWithIncludeResolver.IncludeResolver { get; set; }

        Operator IElasticQueryVisitorContext.DefaultOperator { get; set; }
        bool IElasticQueryVisitorContext.UseScoring { get; set; }
        string IElasticQueryVisitorContext.DefaultField { get; set; }
        Func<string, IProperty> IElasticQueryVisitorContext.GetPropertyMappingFunc { get; set; }

        private DateRange GetDateRange() {
            var rangeQueries = new List<IDateRangeQuery> {
                this.GetSourceAs<IDateRangeQuery>(),
                this.GetSourceAs<ISystemFilterQuery>()?.SystemFilter as IDateRangeQuery
            };

            foreach (var query in rangeQueries) {
                if (query == null)
                    continue;

                foreach (DateRange dateRange in query.DateRanges) {
                    if (dateRange.UseDateRange)
                        return dateRange;
                }
            }

            return null;
        }
    }

    public class ContextType {
        public const string SystemFilter = "SystemFilter";
        public const string Child = "Child";
        public const string Parent = "Parent";
        public const string Default = "Default";
    }

    public interface IQueryBuilderContext {
        IQueryBuilderContext Parent { get; }
        IRepositoryQuery Source { get; }
        ICommandOptions Options { get; }
        QueryContainer Query { get; set; }
        QueryContainer Filter { get; set; }
        string Type { get; }
        IDictionary<string, object> Data { get; }
    }

    public static class QueryBuilderContextExtensions {
        public static TQuery GetSourceAs<TQuery>(this IQueryBuilderContext context) where TQuery : class, IRepositoryQuery {
            return context.Source as TQuery;
        }

        public static TOptions GetOptionsAs<TOptions>(this IQueryBuilderContext context) where TOptions : class, ICommandOptions {
            return context.Options as TOptions;
        }

        public static void SetTimeZone(this IQueryBuilderContext context, string timeZone)
        {
            context.Data["timezone"] = timeZone;
        }
    }

    public static class ElasticQueryBuilderExtensions {
        public static async Task<QueryContainer> BuildQueryAsync<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, ICommandOptions options, SearchDescriptor<T> search) where T : class, new() {
            var ctx = new QueryBuilderContext<T>(query, options, search);
            await builder.BuildAsync(ctx).AnyContext();

            return new BoolQuery {
                Must = new[] { ctx.Query },
                Filter = new[] { ctx.Filter }
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