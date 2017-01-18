using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IElasticQueryBuilder {
        Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new();
    }

    public class QueryBuilderContext<T> : IQueryBuilderContext, IElasticQueryVisitorContext, IQueryVisitorContextWithAliasResolver, IQueryVisitorContextWithIncludeResolver where T : class, new() {
        public QueryBuilderContext(IRepositoryQuery source, IQueryOptions options, SearchDescriptor<T> search = null, IQueryBuilderContext parentContext = null, string type = null) {
            Source = source;
            Options = options;
            Search = search ?? new SearchDescriptor<T>();
            Parent = parentContext;
            Type = type ?? ContextType.Default;
            var elasticQueryOptions = options as IElasticQueryOptions;
            if (elasticQueryOptions != null)
                ((IQueryVisitorContextWithAliasResolver)this).RootAliasResolver = elasticQueryOptions.RootAliasResolver;
        }

        public string Type { get; }
        public IQueryBuilderContext Parent { get; }
        public IRepositoryQuery Source { get; }
        public IQueryOptions Options { get; }
        public QueryContainer Query { get; set; }
        public FilterContainer Filter { get; set; }
        public SearchDescriptor<T> Search { get; }
        public IDictionary<string, object> Data { get; } = new Dictionary<string, object>();

        AliasResolver IQueryVisitorContextWithAliasResolver.RootAliasResolver { get; set; }
        Func<string, Task<string>> IQueryVisitorContextWithIncludeResolver.IncludeResolver { get; set; }

        Operator IElasticQueryVisitorContext.DefaultOperator { get; set; }
        string IElasticQueryVisitorContext.DefaultField { get; set; }
        Func<string, IElasticType> IElasticQueryVisitorContext.GetFieldMappingFunc { get; set; }

        public TQuery GetSourceAs<TQuery>() where TQuery : class {
            return Source as TQuery;
        }

        public TOptions GetOptionsAs<TOptions>() where TOptions : class {
            return Options as TOptions;
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
        IQueryOptions Options { get; }
        QueryContainer Query { get; set; }
        FilterContainer Filter { get; set; }
        string Type { get; }
        IDictionary<string, object> Data { get; }
    }

    public static class QueryBuilderContextExtensions {
        public static TQuery GetSourceAs<TQuery>(this IQueryBuilderContext context) where TQuery : class {
            return context.Source as TQuery;
        }

        public static TOptions GetOptionsAs<TOptions>(this IQueryBuilderContext context) where TOptions : class {
            return context.Options as TOptions;
        }
    }

    public static class ElasticQueryBuilderExtensions {
        public static async Task<QueryContainer> BuildQueryAsync<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, IQueryOptions options, SearchDescriptor<T> search) where T : class, new() {
            var ctx = new QueryBuilderContext<T>(query, options, search);
            await builder.BuildAsync(ctx).AnyContext();

            return new FilteredQuery {
                Filter = ctx.Filter,
                Query = ctx.Query
            };
        }

        public static async Task ConfigureSearchAsync<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, IQueryOptions options, SearchDescriptor<T> search) where T : class, new() {
            if (search == null)
                throw new ArgumentNullException(nameof(search));

            search.Query(await builder.BuildQueryAsync(query, options, search).AnyContext());
        }
    }
}