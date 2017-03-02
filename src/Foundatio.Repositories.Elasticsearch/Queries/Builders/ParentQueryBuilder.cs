using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories {
    public static class ParentQueryExtensions {
        internal const string ParentQueryKey = "@ParentQuery";

        public static T ParentQuery<T>(this T query, IRepositoryQuery parentQuery) where T : IRepositoryQuery {
            if (parentQuery == null)
                throw new ArgumentNullException(nameof(parentQuery));

            return query.BuildOption(ParentQueryKey, parentQuery);
        }

        public static T ParentQuery<T>(this T query, RepositoryQueryDescriptor parentQuery) where T : IRepositoryQuery {
            if (parentQuery == null)
                throw new ArgumentNullException(nameof(parentQuery));

            return query.BuildOption(ParentQueryKey, parentQuery.Configure());
        }

        public static IRepositoryQuery<TChild> ParentQuery<TChild, TParent>(this IRepositoryQuery<TChild> query, RepositoryQueryDescriptor<TParent> parentQuery) where TChild : class where TParent : class {
            if (parentQuery == null)
                throw new ArgumentNullException(nameof(parentQuery));

            return query.BuildOption(ParentQueryKey, parentQuery.Configure());
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadParentQueryExtensions {
        public static IRepositoryQuery GetParentQuery(this IRepositoryQuery query) {
            return query.SafeGetOption<IRepositoryQuery>(ParentQueryExtensions.ParentQueryKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ParentQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public ParentQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var options = ctx.Options.GetElasticTypeSettings();
            if (options.HasParent == false)
                return;

            var parentQuery = ctx.Source.GetParentQuery();
            bool hasIds = ctx.Source.GetIds().Count > 0;

            // even if no parent query has been set, run it through to get soft delete filter
            if (options.ParentSupportsSoftDeletes && hasIds == false && parentQuery == null)
                parentQuery = new RepositoryQuery();

            if (parentQuery == null)
                return;

            var parentType = options.ChildType.GetParentIndexType();
            if (parentType == null)
                throw new ApplicationException("ParentIndexTypeName on child index type must match the name of the parent type.");

            var parentOptions = new CommandOptions().ElasticType(parentType);

            var parentContext = new QueryBuilderContext<object>(parentQuery, parentOptions, null, ctx, ContextType.Parent);
            await _queryBuilder.BuildAsync(parentContext).AnyContext();

            if ((parentContext.Query == null || ((IQueryContainer)parentContext.Query).IsConditionless)
                && (parentContext.Filter == null || ((IQueryContainer)parentContext.Filter).IsConditionless))
                return;

            ctx.Filter &= new HasParentQuery {
                Type = parentType.Name,
                Query = new BoolQuery {
                    Must = new QueryContainer[] { parentContext.Query },
                    Filter = new QueryContainer[] { parentContext.Filter },
                }
            };
        }
    }
}