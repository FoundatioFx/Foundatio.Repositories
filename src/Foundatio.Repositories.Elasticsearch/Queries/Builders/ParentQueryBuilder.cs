using System;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IParentQuery: IRepositoryQuery {
        IRepositoryQuery ParentQuery { get; set; }
    }

    public class ParentQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public ParentQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var parentQuery = ctx.GetSourceAs<IParentQuery>();
            var hasIds = ctx.GetSourceAs<IIdentityQuery>()?.Ids.Count > 0;
            if (parentQuery == null)
                return;

            var options = ctx.GetOptionsAs<IElasticQueryOptions>();
            IQueryOptions parentOptions = null;

            if (options != null && options.HasParent == false)
                return;

            if (options != null && options.ParentSupportsSoftDeletes && hasIds == false) {
                if (parentQuery.ParentQuery == null)
                    parentQuery.ParentQuery = new ParentQuery();

                var parentType = options.ChildType.GetParentIndexType();
                if (parentType == null)
                    throw new ApplicationException("ParentIndexTypeName on child index type must match the name of the parent type.");

                parentOptions = new ElasticQueryOptions(parentType);
            }

            if (parentQuery.ParentQuery == null)
                return;

            var parentContext = new QueryBuilderContext<object>(parentQuery.ParentQuery, parentOptions);
            _queryBuilder.Build(parentContext);

            if ((parentContext.Query == null || parentContext.Query.IsConditionless)
                && (parentContext.Filter == null || parentContext.Filter.IsConditionless))
                return;

            ctx.Filter &= new HasParentFilter {
                Query = parentContext.Query,
                Filter = parentContext.Filter,
                Type = options?.ChildType?.GetParentIndexType().Name
            };
        }
    }

    public static class ParentQueryExtensions {
        public static TQuery WithParentQuery<TQuery, TParentQuery>(this TQuery query, Func<TParentQuery, TParentQuery> parentQueryFunc) where TQuery : IParentQuery where TParentQuery : class, IRepositoryQuery, new() {
            if (parentQueryFunc == null)
                throw new ArgumentNullException(nameof(parentQueryFunc));

            var parentQuery = query.ParentQuery as TParentQuery ?? new TParentQuery();
            query.ParentQuery = parentQueryFunc(parentQuery);

            return query;
        }

        public static Query WithParentQuery<T>(this Query query, Func<T, T> parentQueryFunc) where T : class, IRepositoryQuery, new() {
            if (parentQueryFunc == null)
                throw new ArgumentNullException(nameof(parentQueryFunc));

            var parentQuery = query.ParentQuery as T ?? new T();
            query.ParentQuery = parentQueryFunc(parentQuery);

            return query;
        }

        public static ElasticQuery WithParentQuery<T>(this ElasticQuery query, Func<T, T> parentQueryFunc) where T : class, IRepositoryQuery, new() {
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