using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories
{
    public static class ParentQueryExtensions
    {
        internal const string ParentQueriesKey = "@ParentQueries";

        public static T ParentQuery<T>(this T query, IRepositoryQuery parentQuery) where T : IRepositoryQuery
        {
            if (parentQuery == null)
                throw new ArgumentNullException(nameof(parentQuery));

            return query.AddCollectionOptionValue(ParentQueriesKey, parentQuery);
        }

        public static T ParentQuery<T>(this T query, RepositoryQueryDescriptor parentQuery) where T : IRepositoryQuery
        {
            if (parentQuery == null)
                throw new ArgumentNullException(nameof(parentQuery));

            return query.AddCollectionOptionValue(ParentQueriesKey, parentQuery.Configure());
        }

        internal const string ParentIdKey = "@ParentId";

        public static T ParentId<T>(this T query, string relation, string parentId) where T : IRepositoryQuery
        {
            return query.BuildOption(ParentIdKey, (Relation: relation, ParentId: parentId));
        }

        internal const string DiscriminatorKey = "@Discriminator";

        public static T Discriminator<T>(this T query, string relation) where T : IRepositoryQuery
        {
            return query.BuildOption(DiscriminatorKey, relation);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadParentQueryExtensions
    {
        public static ICollection<IRepositoryQuery> GetParentQueries(this IRepositoryQuery query)
        {
            return query.SafeGetCollection<IRepositoryQuery>(ParentQueryExtensions.ParentQueriesKey);
        }

        public static (string Relation, string ParentId) GetParentId(this IRepositoryQuery query)
        {
            return query.SafeGetOption<(string Relation, string ParentId)>(ParentQueryExtensions.ParentIdKey);
        }

        public static string GetDiscriminator(this IRepositoryQuery query)
        {
            return query.SafeGetOption<string>(ParentQueryExtensions.DiscriminatorKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class ParentQueryBuilder : IElasticQueryBuilder
    {
        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            var index = ctx.Options.GetElasticIndex();

            var parentId = ctx.Source.GetParentId();
            if (!String.IsNullOrEmpty(parentId.Item2))
                ctx.Filter &= new ParentIdQuery { Id = parentId.ParentId, Type = parentId.Relation };

            string discriminator = ctx.Source.GetDiscriminator();
            if (discriminator != null)
                ctx.Filter &= new TermQuery { Field = "discriminator", Value = discriminator };

            var parentQueries = ctx.Source.GetParentQueries();
            if (parentQueries.Count > 0)
            {
                foreach (var parentQuery in parentQueries)
                {
                    var parentOptions = ctx.Options.Clone();
                    parentOptions.DocumentType(parentQuery.GetDocumentType());
                    parentOptions.ParentDocumentType(null);

                    if (parentQuery.GetDocumentType() == typeof(object))
                        parentQuery.DocumentType(ctx.Options.ParentDocumentType());

                    var parentContext = new QueryBuilderContext<object>(parentQuery, parentOptions, null);

                    await index.QueryBuilder.BuildAsync(parentContext);

                    if (parentContext.Filter != null && ((IQueryContainer)parentContext.Filter).IsConditionless == false)
                        ctx.Filter &= new HasParentQuery
                        {
                            ParentType = parentQuery.GetDocumentType(),
                            Query = new BoolQuery
                            {
                                Filter = new[] { parentContext.Filter }
                            }
                        };

                    if (parentContext.Query != null && ((IQueryContainer)parentContext.Query).IsConditionless == false)
                        ctx.Query &= new HasParentQuery
                        {
                            ParentType = parentQuery.GetDocumentType(),
                            Query = new BoolQuery
                            {
                                Must = new[] { parentContext.Query }
                            }
                        };
                }
            }
        }
    }
}
