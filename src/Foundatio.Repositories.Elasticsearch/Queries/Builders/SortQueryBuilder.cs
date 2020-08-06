using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public static class SortQueryExtensions {
        internal const string SortsKey = "@SortsKey";

        public static T Sort<T>(this T query, Field field, SortOrder? order = null) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue<T, IFieldSort>(SortsKey, new FieldSort { Field = field, Order = order });
        }

        public static T SortDescending<T>(this T query, Field field) where T : IRepositoryQuery {
            return query.Sort(field, SortOrder.Descending);
        }

        public static T SortAscending<T>(this T query, Field field) where T : IRepositoryQuery {
            return query.Sort(field, SortOrder.Ascending);
        }

        public static IRepositoryQuery<T> Sort<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, SortOrder? order = null) where T : class {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, IFieldSort>(SortsKey, new FieldSort { Field = objectPath, Order = order });
        }

        public static IRepositoryQuery<T> SortDescending<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class {
            return query.Sort(objectPath, SortOrder.Descending);
        }

        public static IRepositoryQuery<T> SortAscending<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class {
            return query.Sort(objectPath, SortOrder.Ascending);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadSortQueryExtensions {
        public static ICollection<IFieldSort> GetSorts(this IRepositoryQuery query) {
            return query.SafeGetCollection<IFieldSort>(SortQueryExtensions.SortsKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SortQueryBuilder : IElasticQueryBuilder {
        private const string Id = nameof(IIdentity.Id);

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var sortFields = ctx.Source.GetSorts();
            var index = ctx.Options.GetElasticIndex();
            var resolver = ctx.GetMappingResolver();

            if (ctx.Options.ShouldUseSearchAfterPaging()) {
                string idField = resolver.GetResolvedField(Id) ?? "id";

                // ensure id field is always added to the end of the sort fields list
                if (!sortFields.Any(s => resolver.GetResolvedField(s.SortKey).Equals(idField)))
                    sortFields.Add(new FieldSort { Field = idField });
            }

            if (sortFields.Count <= 0)
                return Task.CompletedTask;

            var resolvedSorts = new List<IFieldSort>();
            foreach (var sort in sortFields) {
                if (sort is FieldSort fieldSort)
                    resolvedSorts.Add(new FieldSort {
                        Field = resolver.GetSortFieldName(fieldSort.Field),
                        IgnoreUnmappedFields = fieldSort.IgnoreUnmappedFields,
                        Missing = fieldSort.Missing,
                        Mode = fieldSort.Mode,
                        Nested = fieldSort.Nested,
                        NumericType = fieldSort.NumericType,
                        Order = fieldSort.Order,
                        UnmappedType = fieldSort.UnmappedType
                    });
                else
                    resolvedSorts.Add(sort);
            }

            if (ctx.Options.HasSearchBefore())
                resolvedSorts.ReverseOrder();

            ctx.Search.Sort(resolvedSorts);

            return Task.CompletedTask;
        }

        protected bool CanSortByField(ISet<string> allowedFields, string field) {
            // allow all fields if an allowed list isn't specified
            if (allowedFields == null || allowedFields.Count == 0)
                return true;

            return allowedFields.Contains(field, StringComparer.OrdinalIgnoreCase);
        }
    }
}