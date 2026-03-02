using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

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
    /// <summary>
    /// Handles search_after paging by collecting sorts from context data,
    /// adding the ID field for uniqueness, and reversing sorts for SearchBefore.
    /// This builder runs last (Int32.MaxValue priority) so it sees all accumulated sorts.
    /// </summary>
    public class SearchAfterQueryBuilder : IElasticQueryBuilder
    {
        private const string Id = nameof(IIdentity.Id);

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            // Get sorts from context data (set by SortQueryBuilder or ExpressionQueryBuilder)
            List<SortOptions> sortFields = null;
            if (ctx.Data.TryGetValue(SortQueryBuilder.SortFieldsKey, out var sortsObj) && sortsObj is List<SortOptions> sorts)
            {
                sortFields = sorts;
            }

            // For search_after paging, we need to ensure we have at least the ID field for uniqueness
            if (ctx.Options.ShouldUseSearchAfterPaging())
            {
                sortFields ??= new List<SortOptions>();

                var resolver = ctx.GetMappingResolver();
                string idField = resolver.GetResolvedField(Id) ?? "_id";

                // Check if id field is already in the sort list
                bool hasIdField = sortFields.Any(s =>
                {
                    if (s?.Field?.Field == null)
                        return false;
                    string fieldName = resolver.GetSortFieldName(s.Field.Field);
                    return fieldName?.Equals(idField) == true;
                });

                if (!hasIdField)
                {
                    sortFields.Add(new FieldSort { Field = idField });
                }

                // Reverse sort orders if searching before
                if (ctx.Options.HasSearchBefore())
                {
                    sortFields = sortFields.Select(s => s.ReverseOrder()).ToList();
                }
            }

            // Apply sorts to search descriptor if we have any
            if (sortFields != null && sortFields.Count > 0)
            {
                ctx.Search.Sort(sortFields);
            }

            return Task.CompletedTask;
        }
    }
}
