using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IElasticQueryBuilder {
        void BuildQuery<T>(object query, object options, ref QueryContainer container) where T : class, new();
        void BuildFilter<T>(object query, object options, ref FilterContainer container) where T : class, new();
        void BuildSearch<T>(object query, object options, ref SearchDescriptor<T> descriptor) where T : class, new();
    }

    public abstract class ElasticQueryBuilderBase : IElasticQueryBuilder {
        public virtual void BuildQuery<T>(object query, object options, ref QueryContainer container) where T : class, new() {
            FilterContainer filter = null;
            BuildFilter<T>(query, options, ref filter);

            container &= new FilteredQuery { Filter = filter };
        }

        public virtual void BuildFilter<T>(object query, object options, ref FilterContainer container) where T : class, new() { }

        public virtual void BuildSearch<T>(object query, object options, ref SearchDescriptor<T> descriptor) where T : class, new() {
            FilterContainer filter = null;
            BuildFilter<T>(query, options, ref filter);

            if (filter != null)
                descriptor.Filter(filter);
        }
    }

    public static class QueryBuilderExtensions {
        public static QueryContainer CreateQuery<T>(this IElasticQueryBuilder builder, object query, object options) where T : class, new() {
            QueryContainer container = null;
            builder.BuildQuery<T>(query, options, ref container);
            return container;
        }

        public static FilterContainer CreateFilter<T>(this IElasticQueryBuilder builder, object query, object options) where T : class, new() {
            FilterContainer container = null;
            builder.BuildFilter<T>(query, options, ref container);
            return container;
        }

        public static SearchDescriptor<T> CreateSearch<T>(this IElasticQueryBuilder builder, object query, object options) where T : class, new() {
            SearchDescriptor<T> search = null;
            builder.BuildSearch<T>(query, options, ref search);
            return search;
        }
    }
}