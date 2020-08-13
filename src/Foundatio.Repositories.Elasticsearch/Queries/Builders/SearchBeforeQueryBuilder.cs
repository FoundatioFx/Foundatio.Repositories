using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public static class SearchBeforeQueryExtensions {
        internal const string SearchAfterPagingKey = "@SearchAfterPaging";
        internal const string SearchAfterKey = "@SearchAfter";
        internal const string SearchBeforeKey = "@SearchBefore";

        public static T SearchAfterPaging<T>(this T options, bool enabled = true) where T : ICommandOptions {
            return options.BuildOption(SearchAfterPagingKey, enabled);
        }

        public static T SearchAfter<T>(this T options, params object[] values) where T : ICommandOptions {
            options.SearchAfterPaging();
            if (values != null && values.Count(v => v != null) > 0) {
                options.Values.Set(SearchAfterKey, values);
            } else {
                options.Values.Remove(SearchAfterKey);
            }

            return options;
        }

        public static T SearchAfterToken<T>(this T options, string searchAfterToken) where T : ICommandOptions {
            options.SearchAfterPaging();
            if (!String.IsNullOrEmpty(searchAfterToken)) {
                var values = FindHitExtensions.DecodeSortToken(searchAfterToken);
                options.Values.Set(SearchAfterKey, values);
            } else {
                options.Values.Remove(SearchAfterKey);
            }

            return options;
        }

        public static T SearchBefore<T>(this T options, params object[] values) where T : ICommandOptions {
            options.SearchAfterPaging();
            if (values != null && values.Count(v => v != null) > 0) {
                options.Values.Set(SearchBeforeKey, values);
            } else {
                options.Values.Remove(SearchBeforeKey);
            }

            return options;
        }

        public static T SearchBeforeToken<T>(this T options, string searchBeforeToken) where T : ICommandOptions {
            options.SearchAfterPaging();
            if (!String.IsNullOrEmpty(searchBeforeToken)) {
                var values = FindHitExtensions.DecodeSortToken(searchBeforeToken);
                options.Values.Set(SearchBeforeKey, values);
            } else {
                options.Values.Remove(SearchBeforeKey);
            }

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadSearchBeforeQueryExtensions {
        public static bool ShouldUseSearchAfterPaging(this ICommandOptions options) {
            return options.SafeGetOption<bool>(SearchBeforeQueryExtensions.SearchAfterPagingKey, false);
        }

        public static object[] GetSearchAfter(this ICommandOptions options) {
            return options.SafeGetOption<object[]>(SearchBeforeQueryExtensions.SearchAfterKey);
        }

        public static bool HasSearchAfter(this ICommandOptions options) {
            var sorts = options.SafeGetOption<object[]>(SearchBeforeQueryExtensions.SearchAfterKey);
            return sorts != null && sorts.Length > 0;
        }

        public static object[] GetSearchBefore(this ICommandOptions options) {
            return options.SafeGetOption<object[]>(SearchBeforeQueryExtensions.SearchBeforeKey);
        }

        public static bool HasSearchBefore(this ICommandOptions options) {
            var sorts = options.SafeGetOption<object[]>(SearchBeforeQueryExtensions.SearchBeforeKey);
            return sorts != null && sorts.Length > 0;
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SearchBeforeQueryBuilder : IElasticQueryBuilder {
        private const string Id = nameof(IIdentity.Id);

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var resolver = ctx.GetMappingResolver();

            if (!ctx.Options.ShouldUseSearchAfterPaging())
                return Task.CompletedTask;
            
            string idField = resolver.GetResolvedField(Id) ?? "id";

            var searchRequest = ctx.Search as ISearchRequest;
            if (searchRequest == null)
                return Task.CompletedTask;

            var sortFields = searchRequest.Sort?.ToList() ?? new List<ISort>();

            // ensure id field is always added to the end of the sort fields list
            if (!sortFields.Any(s => resolver.GetResolvedField(s.SortKey).Equals(idField)))
                ctx.Search.Sort(new[] { new FieldSort { Field = idField } });

            // reverse sort orders on all sorts
            if (ctx.Options.HasSearchBefore())
                sortFields.ReverseOrder();

            return Task.CompletedTask;
        }
    }
}
