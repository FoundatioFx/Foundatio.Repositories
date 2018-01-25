using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public static class SortQueryExtensions {
        internal const string SortsKey = "@SortsKey";

        public static T Sort<T>(this T query, Field field, SortOrder? order = null) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue<T, IFieldSort>(SortsKey, new SortField { Field = field, Order = order });
        }

        public static T SortDescending<T>(this T query, Field field) where T : IRepositoryQuery {
            return query.Sort(field, SortOrder.Descending);
        }

        public static T SortAscending<T>(this T query, Field field) where T : IRepositoryQuery {
            return query.Sort(field, SortOrder.Ascending);
        }

        public static IRepositoryQuery<T> Sort<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, SortOrder? order = null) where T : class {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, IFieldSort>(SortsKey, new SortField { Field = objectPath, Order = order });
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
            
            if (sortFields.Count <= 0) {
                // always use the default sort if using search after paging
                if (ctx.Options.ShouldUseSearchAfterPaging()) {
                    var options = ctx.Options.GetElasticTypeSettings();
                    string fieldName = options.IndexType?.GetFieldName(Id) ?? Id;
                    ctx.Search.Sort(s => s.Field(fieldName, SortOrder.Ascending));
                }
                
                return Task.CompletedTask;
            }

            //var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            //foreach (var sort in sortableQuery.SortFields.Where(s => CanSortByField(opt?.AllowedSortFields, s.Field)))
            //    ctx.Search.Sort(s => s.Field(sort.Field, sort.Order == Foundatio.Repositories.Models.SortOrder.Ascending ? SortOrder.Ascending : SortOrder.Descending));
            ctx.Search.Sort(sortFields);

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