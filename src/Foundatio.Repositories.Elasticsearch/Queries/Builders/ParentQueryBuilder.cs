using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Queries;
using Nest;
using Foundatio.Repositories.Elasticsearch.Options;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IParentQuery: IRepositoryQuery {
        IRepositoryQuery ParentQuery { get; set; }
    }

    public class ParentQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public ParentQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var parentQuery = ctx.GetSourceAs<IParentQuery>();
            bool hasIds = ctx.GetSourceAs<IIdentityQuery>()?.Ids.Count > 0;
            if (parentQuery == null)
                return;

            var options = ctx.GetOptionsAs<IElasticCommandOptions>();
            ICommandOptions parentOptions = null;

            if (options != null && options.HasParent == false)
                return;

            if (options != null && options.ParentSupportsSoftDeletes && hasIds == false) {
                if (parentQuery.ParentQuery == null)
                    parentQuery.ParentQuery = new ParentQuery();

                var parentType = options.ChildType.GetParentIndexType();
                if (parentType == null)
                    throw new ApplicationException("ParentIndexTypeName on child index type must match the name of the parent type.");

                parentOptions = new ElasticCommandOptions(parentType);
            }

            if (parentQuery.ParentQuery == null)
                return;

            var parentContext = new QueryBuilderContext<object>(parentQuery.ParentQuery, parentOptions, null, ctx, ContextType.Parent);
            await _queryBuilder.BuildAsync(parentContext).AnyContext();

            if ((parentContext.Query == null || ((IQueryContainer)parentContext.Query).IsConditionless)
                && (parentContext.Filter == null || ((IQueryContainer)parentContext.Filter).IsConditionless))
                return;

            ctx.Filter &= new HasParentQuery {
                Type = options?.ChildType?.GetParentIndexType().Name,
                Query = new BoolQuery {
                    Must = new QueryContainer[] { parentContext.Query },
                    Filter = new QueryContainer[] { parentContext.Filter },
                }
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