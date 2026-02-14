using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories
{
    public static class SearchAfterQueryExtensions
    {
        internal const string SearchAfterPagingKey = "@SearchAfterPaging";
        internal const string SearchAfterKey = "@SearchAfter";
        internal const string SearchBeforeKey = "@SearchBefore";

        public static T SearchAfterPaging<T>(this T options, bool enabled = true) where T : ICommandOptions
        {
            return options.BuildOption(SearchAfterPagingKey, enabled);
        }

        public static T SearchAfter<T>(this T options, params object[] values) where T : ICommandOptions
        {
            options.SearchAfterPaging();
            if (values != null && values.Count(v => v != null) > 0)
            {
                options.Values.Set(SearchAfterKey, values);
            }
            else
            {
                options.Values.Remove(SearchAfterKey);
            }

            return options;
        }

        public static T SearchAfterToken<T>(this T options, string searchAfterToken) where T : ICommandOptions
        {
            options.SearchAfterPaging();
            if (!String.IsNullOrEmpty(searchAfterToken))
            {
                object[] values = FindHitExtensions.DecodeSortToken(searchAfterToken);
                options.Values.Set(SearchAfterKey, values);
            }
            else
            {
                options.Values.Remove(SearchAfterKey);
            }

            return options;
        }

        public static T SearchBefore<T>(this T options, params object[] values) where T : ICommandOptions
        {
            options.SearchAfterPaging();
            if (values != null && values.Count(v => v != null) > 0)
            {
                options.Values.Set(SearchBeforeKey, values);
            }
            else
            {
                options.Values.Remove(SearchBeforeKey);
            }

            return options;
        }

        public static T SearchBeforeToken<T>(this T options, string searchBeforeToken) where T : ICommandOptions
        {
            options.SearchAfterPaging();
            if (!String.IsNullOrEmpty(searchBeforeToken))
            {
                object[] values = FindHitExtensions.DecodeSortToken(searchBeforeToken);
                options.Values.Set(SearchBeforeKey, values);
            }
            else
            {
                options.Values.Remove(SearchBeforeKey);
            }

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadSearchAfterQueryExtensions
    {
        public static bool ShouldUseSearchAfterPaging(this ICommandOptions options)
        {
            return options.SafeGetOption<bool>(SearchAfterQueryExtensions.SearchAfterPagingKey, false);
        }

        public static object[] GetSearchAfter(this ICommandOptions options)
        {
            return options.SafeGetOption<object[]>(SearchAfterQueryExtensions.SearchAfterKey);
        }

        public static bool HasSearchAfter(this ICommandOptions options)
        {
            object[] sorts = options.SafeGetOption<object[]>(SearchAfterQueryExtensions.SearchAfterKey);
            return sorts != null && sorts.Length > 0;
        }

        public static object[] GetSearchBefore(this ICommandOptions options)
        {
            return options.SafeGetOption<object[]>(SearchAfterQueryExtensions.SearchBeforeKey);
        }

        public static bool HasSearchBefore(this ICommandOptions options)
        {
            object[] sorts = options.SafeGetOption<object[]>(SearchAfterQueryExtensions.SearchBeforeKey);
            return sorts != null && sorts.Length > 0;
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class SearchAfterQueryBuilder : IElasticQueryBuilder
    {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            if (!ctx.Options.ShouldUseSearchAfterPaging())
                return Task.CompletedTask;

            if (ctx.Search is not ISearchRequest searchRequest)
                return Task.CompletedTask;

            // reverse sort orders on all sorts for search before
            if (ctx.Options.HasSearchBefore())
                searchRequest.Sort?.ReverseOrder();

            return Task.CompletedTask;
        }
    }
}
