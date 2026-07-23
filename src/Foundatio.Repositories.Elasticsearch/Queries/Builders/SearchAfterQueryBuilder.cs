using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Serializer;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories
{
    public enum SearchAfterPagingMode
    {
        Live,
        PointInTime
    }

    public static class SearchAfterQueryExtensions
    {
        internal const string SearchAfterPagingKey = "@SearchAfterPaging";
        internal const string SearchAfterPagingModeKey = "@SearchAfterPagingMode";
        internal const string SearchAfterKey = "@SearchAfter";
        internal const string SearchBeforeKey = "@SearchBefore";
        internal const string PointInTimeIdKey = "@PointInTimeId";
        internal const string RepoOwnedPointInTimeKey = "@RepoOwnedPointInTime";
        internal const string UnstableSortWarnedKey = "@SearchAfterUnstableSortWarned";

        public static T SearchAfterPaging<T>(this T options, bool enabled = true) where T : ICommandOptions
        {
            return options.BuildOption(SearchAfterPagingKey, enabled);
        }

        public static T SearchAfterPaging<T>(this T options, SearchAfterPagingMode mode, bool enabled = true) where T : ICommandOptions
        {
            options.BuildOption(SearchAfterPagingKey, enabled);
            return options.BuildOption(SearchAfterPagingModeKey, enabled ? mode : SearchAfterPagingMode.Live);
        }

        public static T PointInTimeId<T>(this T options, string? pointInTimeId) where T : ICommandOptions
        {
            if (!String.IsNullOrEmpty(pointInTimeId))
                options.Values.Set(PointInTimeIdKey, pointInTimeId);
            else
                options.Values.Remove(PointInTimeIdKey);

            return options;
        }

        internal static T RepoOwnedPointInTime<T>(this T options, bool repoOwned = true) where T : ICommandOptions
        {
            return options.BuildOption(RepoOwnedPointInTimeKey, repoOwned);
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

        public static T SearchAfterToken<T>(this T options, string? searchAfterToken, ITextSerializer serializer) where T : ICommandOptions
        {
            options.SearchAfterPaging();
            if (!String.IsNullOrEmpty(searchAfterToken))
            {
                object[]? values = FindHitExtensions.DecodeSortToken(searchAfterToken, serializer);
                if (values is not null)
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

        public static T SearchBeforeToken<T>(this T options, string? searchBeforeToken, ITextSerializer serializer) where T : ICommandOptions
        {
            options.SearchAfterPaging();
            if (!String.IsNullOrEmpty(searchBeforeToken))
            {
                object[]? values = FindHitExtensions.DecodeSortToken(searchBeforeToken, serializer);
                if (values is not null)
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

        public static SearchAfterPagingMode GetSearchAfterPagingMode(this ICommandOptions options)
        {
            return options.SafeGetOption(SearchAfterQueryExtensions.SearchAfterPagingModeKey, SearchAfterPagingMode.Live);
        }

        public static bool ShouldUseSearchAfterPagingPointInTime(this ICommandOptions options)
        {
            return options.ShouldUseSearchAfterPaging() && options.GetSearchAfterPagingMode() is SearchAfterPagingMode.PointInTime;
        }

        public static string? GetPointInTimeId(this ICommandOptions options)
        {
            return options.SafeGetOption<string?>(SearchAfterQueryExtensions.PointInTimeIdKey);
        }

        public static bool HasPointInTimeId(this ICommandOptions options)
        {
            return !String.IsNullOrEmpty(options.GetPointInTimeId());
        }

        internal static bool IsRepoOwnedPointInTime(this ICommandOptions options)
        {
            return options.SafeGetOption<bool>(SearchAfterQueryExtensions.RepoOwnedPointInTimeKey, false);
        }

        public static object[]? GetSearchAfter(this ICommandOptions options)
        {
            return options.SafeGetOption<object[]>(SearchAfterQueryExtensions.SearchAfterKey);
        }

        public static bool HasSearchAfter(this ICommandOptions options)
        {
            object[]? sorts = options.SafeGetOption<object[]>(SearchAfterQueryExtensions.SearchAfterKey);
            return sorts is { Length: > 0 };
        }

        public static object[]? GetSearchBefore(this ICommandOptions options)
        {
            return options.SafeGetOption<object[]>(SearchAfterQueryExtensions.SearchBeforeKey);
        }

        public static bool HasSearchBefore(this ICommandOptions options)
        {
            object[]? sorts = options.SafeGetOption<object[]>(SearchAfterQueryExtensions.SearchBeforeKey);
            return sorts is { Length: > 0 };
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
    /// <remarks>
    /// Also logs a warning when Live-mode search_after paging is combined with an unstable sort
    /// key (<c>_doc</c> or <c>_score</c>), since those keys are only stable within a Point-In-Time
    /// and can otherwise cause paging to silently skip documents or stop early. Because the same
    /// <see cref="ICommandOptions"/> instance is reused across <c>FindResults.NextPageAsync()</c>
    /// calls, the warning is only logged once per paging session (i.e. on the first page).
    /// </remarks>
    public class SearchAfterQueryBuilder : IElasticQueryBuilder
    {
        private const string Id = nameof(IIdentity.Id);

        // Internal Lucene/relevance sort keys that are not stable across index refreshes or
        // segment merges. Using them as a search_after cursor in Live paging mode can silently
        // skip documents or terminate paging early while the index is being written to.
        private static readonly HashSet<string> UnstableSortFields = new(StringComparer.Ordinal)
        {
            "_doc",
            "_score"
        };

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            // Get sorts from context data (set by SortQueryBuilder or ExpressionQueryBuilder)
            List<SortOptions>? sortFields = null;
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

                // Live search_after paging with an unstable sort key (e.g. _doc, _score) is only safe
                // within a Point-In-Time: index refreshes and segment merges can invalidate the cursor,
                // silently skipping documents or stopping paging early. Not applicable in PointInTime
                // mode, where a frozen view keeps these sort keys stable.
                bool warnOnUnstableSort = ctx.Options.GetSearchAfterPagingMode() is SearchAfterPagingMode.Live;

                // The same ICommandOptions instance is reused across FindResults.NextPageAsync() calls,
                // so BuildAsync runs once per page. Only warn on the first page to avoid flooding logs
                // with the same warning for every page of a long-running paging session.
                bool alreadyWarned = ctx.Options.SafeGetOption<bool>(SearchAfterQueryExtensions.UnstableSortWarnedKey, false);

                // Single pass: resolve each sort's unstable field name (if any) once to check
                // both for the ID tiebreaker and for unstable sort keys, avoiding redundant
                // resolver calls. SortOptions is a discriminated union: _doc/_score can arrive
                // either as a FieldSort with a literal field name, or as the ES client's typed
                // Doc/Score variants, so both shapes need to be checked.
                bool hasIdField = false;
                foreach (var sort in sortFields)
                {
                    string? fieldName;
                    bool isIdField = false;

                    if (sort?.Field?.Field is { } sortField)
                    {
                        fieldName = resolver.GetSortFieldName(sortField);
                        if (fieldName is null)
                            continue;

                        isIdField = String.Equals(fieldName, idField, StringComparison.Ordinal);
                    }
                    else if (sort?.Doc is not null)
                    {
                        fieldName = "_doc";
                    }
                    else if (sort?.Score is not null)
                    {
                        fieldName = "_score";
                    }
                    else
                    {
                        continue;
                    }

                    if (isIdField)
                        hasIdField = true;

                    if (warnOnUnstableSort && !alreadyWarned && UnstableSortFields.Contains(fieldName))
                    {
                        var logger = (ctx.Options.GetElasticIndex() as IHaveLogger)?.Logger ?? NullLogger.Instance;
                        logger.LogWarning("Sorting by {SortField} with Live search_after paging is unstable: {SortField} is not stable across index refreshes or segment merges, so the cursor can become invalid and paging may silently stop early (especially while writing to the index being paged). Sort by a stable, unique field or use SearchAfterPaging(SearchAfterPagingMode.PointInTime).", fieldName, fieldName);
                        ctx.Options.Values.Set(SearchAfterQueryExtensions.UnstableSortWarnedKey, true);
                        alreadyWarned = true;
                    }
                }

                if (!hasIdField)
                {
                    sortFields.Add(new FieldSort { Field = idField });
                }

                // Reverse sort orders if searching before
                if (ctx.Options.HasSearchBefore())
                {
                    sortFields = sortFields.Select(s => s.ReverseOrder()!).ToList();
                }
            }

            // Apply sorts to search descriptor if we have any
            if (sortFields is { Count: > 0 })
            {
                ctx.Search.Sort(sortFields);
            }

            return Task.CompletedTask;
        }
    }
}
