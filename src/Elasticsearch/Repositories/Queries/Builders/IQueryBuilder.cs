using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IQueryBuilder {
        void BuildQuery<T>(object query, object options, ref QueryContainer container) where T : class, new();
        void BuildFilter<T>(object query, object options, ref FilterContainer container) where T : class, new();
        void BuildSearch<T>(object query, object options, ref SearchDescriptor<T> descriptor) where T : class, new();
    }

    public abstract class QueryBuilderBase : IQueryBuilder {
        public virtual void BuildQuery<T>(object query, object options, ref QueryContainer container) where T : class, new() { }
        public virtual void BuildFilter<T>(object query, object options, ref FilterContainer container) where T : class, new() { }
        public virtual void BuildSearch<T>(object query, object options, ref SearchDescriptor<T> descriptor) where T : class, new() { }
    }

    public static class QueryBuilderExtensions {
        public static QueryContainer CreateQuery<T>(this IQueryBuilder builder, object query, object options) where T : class, new() {
            QueryContainer container = null;
            builder.BuildQuery<T>(query, options, ref container);
            return container;
        }

        public static FilterContainer CreateFilter<T>(this IQueryBuilder builder, object query, object options) where T : class, new() {
            FilterContainer container = null;
            builder.BuildFilter<T>(query, options, ref container);
            return container;
        }

        public static SearchDescriptor<T> CreateSearch<T>(this IQueryBuilder builder, object query, object options) where T : class, new() {
            SearchDescriptor<T> search = null;
            builder.BuildSearch<T>(query, options, ref search);
            return search;
        }
    }
}