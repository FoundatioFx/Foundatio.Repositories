using System;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IParentQuery: IRepositoryQuery {
        ITypeQuery ParentQuery { get; set; }
    }

    public class ParentQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public ParentQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var parentQuery = ctx.GetSourceAs<IParentQuery>();
            if (parentQuery == null)
                return;

            var options = ctx.GetOptionsAs<IElasticQueryOptions>();
            IQueryOptions parentOptions = null;

            if (options != null && options.HasParent == false)
                return;

            if (parentQuery.ParentQuery == null && options != null && options.ParentSupportsSoftDeletes) {
                parentQuery.ParentQuery = new ParentQuery();
                var parentType = options.ChildType.Index.IndexTypes.FirstOrDefault(i => i.Name == options.ChildType.ParentIndexTypeName);
                if (parentType == null)
                    throw new ApplicationException("ParentIndexTypeName on child index type must match the name of the parent type.");

                parentOptions = new ElasticQueryOptions(parentType) {
                    SupportsSoftDeletes = true
                };
            }

            if (parentQuery.ParentQuery == null)
                return;

            var query = _queryBuilder.BuildQuery<T>(parentQuery.ParentQuery, parentOptions);
            var filter = _queryBuilder.BuildFilter<T>(parentQuery.ParentQuery, parentOptions);
            ctx.Filter &= new HasParentFilter {
                Query = query,
                Filter = filter,
                Type = options?.ChildType?.ParentIndexTypeName
            };
        }
    }

    public static class ParentQueryExtensions {
        public static TQuery WithParentQuery<TQuery, TParentQuery>(this TQuery query, Func<TParentQuery, TParentQuery> parentQueryFunc) where TQuery : IParentQuery where TParentQuery : class, ITypeQuery, new() {
            if (parentQueryFunc == null)
                throw new ArgumentNullException(nameof(parentQueryFunc));

            var parentQuery = query.ParentQuery as TParentQuery ?? new TParentQuery();
            query.ParentQuery = parentQueryFunc(parentQuery);

            return query;
        }

        public static Query WithParentQuery<T>(this Query query, Func<T, T> parentQueryFunc) where T : class, ITypeQuery, new() {
            if (parentQueryFunc == null)
                throw new ArgumentNullException(nameof(parentQueryFunc));

            var parentQuery = query.ParentQuery as T ?? new T();
            query.ParentQuery = parentQueryFunc(parentQuery);

            return query;
        }

        public static ElasticQuery WithParentQuery<T>(this ElasticQuery query, Func<T, T> parentQueryFunc) where T : class, ITypeQuery, new() {
            if (parentQueryFunc == null)
                throw new ArgumentNullException(nameof(parentQueryFunc));

            var parentQuery = query.ParentQuery as T ?? new T();
            query.ParentQuery = parentQueryFunc(parentQuery);

            return query;
        }

        public static ElasticQuery WithParentQuery(this ElasticQuery query, Func<ParentQuery, ParentQuery> parentQueryFunc) {
            if (parentQueryFunc == null)
                throw new ArgumentNullException(nameof(parentQueryFunc));

            var parentQuery = query.ParentQuery as ParentQuery ?? new ParentQuery();
            query.ParentQuery = parentQueryFunc(parentQuery);

            return query;
        }
    }
}