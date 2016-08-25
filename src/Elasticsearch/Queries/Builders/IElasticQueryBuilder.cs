using System;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IElasticQueryBuilder {
        void Build<T>(QueryBuilderContext<T> ctx) where T : class, new();
    }

    public class QueryBuilderContext<T> where T : class, new() {
        public QueryBuilderContext(IRepositoryQuery source, IQueryOptions options, SearchDescriptor<T> search = null) {
            Source = source;
            Options = options;
            Search = search ?? new SearchDescriptor<T>();
        }

        public IRepositoryQuery Source { get; }
        public IQueryOptions Options { get; }
        public QueryContainer Query { get; set; }
        public FilterContainer Filter { get; set; }
        public SearchDescriptor<T> Search { get; }

        public TQuery GetSourceAs<TQuery>() where TQuery : class {
            return Source as TQuery;
        }

        public TOptions GetOptionsAs<TOptions>() where TOptions : class {
            return Options as TOptions;
        }
    }

    public static class ElasticQueryBuilderExtensions {
        public static QueryContainer BuildQuery<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, IQueryOptions options) where T : class, new() {
            var ctx = new QueryBuilderContext<T>(query, options);
            builder.Build(ctx);

            return ctx.Query;
        }

        public static FilterContainer BuildFilter<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, IQueryOptions options) where T : class, new() {
            var ctx = new QueryBuilderContext<T>(query, options);
            builder.Build(ctx);

            return ctx.Filter;
        }

        public static void BuildSearch<T>(this IElasticQueryBuilder builder, IRepositoryQuery query, IQueryOptions options, ref SearchDescriptor<T> search) where T : class, new() {
            if (search == null)
                search = new SearchDescriptor<T>();

            var ctx = new QueryBuilderContext<T>(query, options, search);
            builder.Build(ctx);

            if (ctx.Query != null)
                search.Query(ctx.Query);
            
            if (ctx.Filter != null)
                search.Query(ctx.Query &= new FilteredQuery { Filter = ctx.Filter });
        }
    }
}