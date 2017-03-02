using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public class ChildQuery {
        public TypeName Type { get; set; }
        public IRepositoryQuery Query { get; set; }
    }

    public static class ChildQueryExtensions {
        internal const string ChildQueryKey = "@ChildQuery";

        public static T ChildQuery<T>(this T query, TypeName childType, IRepositoryQuery childQuery) where T : IRepositoryQuery {
            if (childType == null)
                throw new ArgumentNullException(nameof(childType));
            if (childQuery == null)
                throw new ArgumentNullException(nameof(childQuery));

            return query.BuildOption(ChildQueryKey, new ChildQuery { Type = childType, Query = childQuery });
        }

        public static T ChildQuery<T>(this T query, TypeName childType, RepositoryQueryDescriptor childQuery) where T : IRepositoryQuery {
            if (childType == null)
                throw new ArgumentNullException(nameof(childType));
            if (childQuery == null)
                throw new ArgumentNullException(nameof(childQuery));

            return query.BuildOption(ChildQueryKey, new ChildQuery { Type = childType, Query = childQuery.Configure() });
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadChildQueryExtensions {
        public static ChildQuery GetChildQuery(this IRepositoryQuery query) {
            return query.SafeGetOption<ChildQuery>(ChildQueryExtensions.ChildQueryKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ChildQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryBuilder _queryBuilder;

        public ChildQueryBuilder(ElasticQueryBuilder queryBuilder) {
            _queryBuilder = queryBuilder;
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var childQuery = ctx.Source.GetChildQuery();
            if (childQuery == null)
                return;
            
            var childContext = new QueryBuilderContext<T>(childQuery.Query, ctx.Options, null, ctx, ContextType.Child);
            await _queryBuilder.BuildAsync(childContext).AnyContext();

            if ((childContext.Query == null || ((IQueryContainer)childContext.Query).IsConditionless)
                && (childContext.Filter == null || ((IQueryContainer)childContext.Filter).IsConditionless))
                return;

            ctx.Filter &= new HasChildQuery {
                Type = childQuery.Type,
                Query = new BoolQuery {
                    Must = new QueryContainer[] { childContext.Query },
                    Filter = new QueryContainer[] { childContext.Filter },
                }
            };
        }
    }
}