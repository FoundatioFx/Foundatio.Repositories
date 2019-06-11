using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public static class ParentQueryExtensions {
        internal const string ParentQueriesKey = "@ParentQueries";

        public static T ParentQuery<T>(this T query, IRepositoryQuery parentQuery) where T : IRepositoryQuery {
            if (parentQuery == null)
                throw new ArgumentNullException(nameof(parentQuery));

            return query.AddCollectionOptionValue(ParentQueriesKey, parentQuery);
        }

        public static T ParentQuery<T>(this T query, RepositoryQueryDescriptor parentQuery) where T : IRepositoryQuery {
            if (parentQuery == null)
                throw new ArgumentNullException(nameof(parentQuery));

            return query.AddCollectionOptionValue(ParentQueriesKey, parentQuery.Configure());
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadParentQueryExtensions {
        public static ICollection<IRepositoryQuery> GetParentQueries(this IRepositoryQuery query) {
            return query.SafeGetCollection<IRepositoryQuery>(ParentQueryExtensions.ParentQueriesKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class ParentQueryBuilder : IElasticQueryBuilder {
        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var index = ctx.Options.GetElasticIndex();

            var parentQueries = ctx.Source.GetParentQueries();
            if (parentQueries.Count > 0) {
                foreach (var parentQuery in parentQueries) {
                    var parentOptions = ctx.Options.Clone();
                    parentOptions.DocumentType(parentQuery.GetDocumentType());
                    // TODO: allow parent document type to be set in the options so that it's not required to be set on each query
                    
                    var parentContext = new QueryBuilderContext<object>(parentQuery, parentOptions, null);

                    await index.QueryBuilder.BuildAsync(parentContext);

                    if (parentContext.Filter != null && ((IQueryContainer)parentContext.Filter).IsConditionless == false)
                        ctx.Filter &= new HasParentQuery {
                            ParentType = parentQuery.GetDocumentType(), Query = new BoolQuery {
                                Filter = new[] { parentContext.Filter }
                            }
                        };

                    if (parentContext.Query != null && ((IQueryContainer)parentContext.Query).IsConditionless == false)
                        ctx.Query &= new HasParentQuery {
                            ParentType = parentQuery.GetDocumentType(), Query = new BoolQuery {
                                Must = new[] { parentContext.Query }
                            }
                        };
                }
            }
        }
    }
}