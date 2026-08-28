using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
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
            if (!enabled)
                return ResetPagingSession(options);

            return options.BuildOption(SearchAfterPagingKey, true);
        }

        public static T SearchAfterPaging<T>(this T options, SearchAfterPagingMode mode, bool enabled = true) where T : ICommandOptions
        {
            if (!enabled)
                return ResetPagingSession(options);

            options.BuildOption(SearchAfterPagingKey, true);
            return options.BuildOption(SearchAfterPagingModeKey, mode);
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

        /// <summary>
        /// Pages results forwards from the supplied sort values, enabling search-after paging.
        /// </summary>
        /// <param name="options">The command options.</param>
        /// <param name="values">The sort values of the last hit on the previous page, in sort order.</param>
        /// <remarks>
        /// Calling this method always enables search-after paging, even when no cursor is stored.
        /// Passing no values, a null array reference, or an array whose elements are all null clears
        /// any stored cursor. A cursor needs at least one non-null sort value to be meaningful; when
        /// paging on a model without an id tiebreaker whose only sort field can be missing (producing
        /// all-null cursors), use <see cref="SearchAfterToken"/> instead -- tokens round-trip null
        /// values exactly.
        /// </remarks>
        public static T SearchAfter<T>(this T options, params object?[]? values) where T : ICommandOptions
        {
            bool hasCursor = values is { Length: > 0 } && Array.Exists(values, value => value is not null);
            return SetCursor(options, SearchAfterKey, SearchBeforeKey, values, hasCursor);
        }

        public static T SearchAfterToken<T>(this T options, string? searchAfterToken, ITextSerializer serializer) where T : ICommandOptions
        {
            ArgumentNullException.ThrowIfNull(serializer);

            object?[]? values = null;
            if (!String.IsNullOrEmpty(searchAfterToken))
                values = FindHitExtensions.DecodeSortToken(searchAfterToken, serializer);

            return SetCursor(options, SearchAfterKey, SearchBeforeKey, values, values is not null);
        }

        /// <summary>
        /// Pages results backwards from the supplied sort values, enabling search-after paging in reverse.
        /// </summary>
        /// <param name="options">The command options.</param>
        /// <param name="values">The sort values of the first hit on the next page, in sort order.</param>
        /// <remarks>
        /// Calling this method always enables search-after paging, even when no cursor is stored.
        /// Passing no values, a null array reference, or an array whose elements are all null clears
        /// any stored cursor. A cursor needs at least one non-null sort value to be meaningful; when
        /// paging on a model without an id tiebreaker whose only sort field can be missing (producing
        /// all-null cursors), use <see cref="SearchBeforeToken"/> instead -- tokens round-trip null
        /// values exactly.
        /// </remarks>
        public static T SearchBefore<T>(this T options, params object?[]? values) where T : ICommandOptions
        {
            bool hasCursor = values is { Length: > 0 } && Array.Exists(values, value => value is not null);
            return SetCursor(options, SearchBeforeKey, SearchAfterKey, values, hasCursor);
        }

        public static T SearchBeforeToken<T>(this T options, string? searchBeforeToken, ITextSerializer serializer) where T : ICommandOptions
        {
            ArgumentNullException.ThrowIfNull(serializer);

            object?[]? values = null;
            if (!String.IsNullOrEmpty(searchBeforeToken))
                values = FindHitExtensions.DecodeSortToken(searchBeforeToken, serializer);

            return SetCursor(options, SearchBeforeKey, SearchAfterKey, values, values is not null);
        }

        private static T SetCursor<T>(T options, string cursorKey, string oppositeCursorKey, object?[]? values, bool hasCursor) where T : ICommandOptions
        {
            options.SearchAfterPaging();
            options.Values.Remove(oppositeCursorKey);

            if (hasCursor && values is not null)
                options.Values.Set(cursorKey, values);
            else
                options.Values.Remove(cursorKey);

            return options;
        }

        private static T ResetPagingSession<T>(T options) where T : ICommandOptions
        {
            options.BuildOption(SearchAfterPagingKey, false);
            options.BuildOption(SearchAfterPagingModeKey, SearchAfterPagingMode.Live);
            options.Values.Remove(SearchAfterKey);
            options.Values.Remove(SearchBeforeKey);
            options.Values.Remove(PointInTimeIdKey);
            options.Values.Remove(RepoOwnedPointInTimeKey);
            options.Values.Remove(UnstableSortWarnedKey);
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

        public static object?[]? GetSearchAfter(this ICommandOptions options)
        {
            return options.SafeGetOption<object?[]>(SearchAfterQueryExtensions.SearchAfterKey);
        }

        public static bool HasSearchAfter(this ICommandOptions options)
        {
            object?[]? sorts = options.SafeGetOption<object?[]>(SearchAfterQueryExtensions.SearchAfterKey);
            return sorts is { Length: > 0 };
        }

        public static object?[]? GetSearchBefore(this ICommandOptions options)
        {
            return options.SafeGetOption<object?[]>(SearchAfterQueryExtensions.SearchBeforeKey);
        }

        public static bool HasSearchBefore(this ICommandOptions options)
        {
            object?[]? sorts = options.SafeGetOption<object?[]>(SearchAfterQueryExtensions.SearchBeforeKey);
            return sorts is { Length: > 0 };
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    /// <summary>
    /// Handles search_after paging by collecting sorts from context data, ensuring a deterministic
    /// tiebreaker when possible, and reversing sorts for SearchBefore.
    /// This builder runs last (Int32.MaxValue priority) so it sees all accumulated sorts.
    /// </summary>
    /// <remarks>
    /// Also logs a warning when Live-mode search_after paging is combined with an unstable sort
    /// key (<c>_doc</c> or <c>_score</c>), since those keys are only stable within a Point-In-Time
    /// and can otherwise cause paging to silently skip documents or stop early. Because the same
    /// <see cref="ICommandOptions"/> instance is reused across <c>FindResults.NextPageAsync()</c>
    /// calls, the warning is only logged once per paging session (i.e. on the first page).
    /// See <see cref="IdTiebreakerField.TryEnsure{T}"/> for when the id tiebreaker is skipped
    /// entirely (models without <see cref="IIdentity"/>, or indexes that opt out via
    /// <see cref="Foundatio.Repositories.Elasticsearch.Configuration.IIndex.HasSortableIdField"/>);
    /// Live-mode callers of a search_after query against such a model or index must supply their
    /// own unique, sortable field(s) to keep the cursor stable; the builder throws
    /// <see cref="QueryValidationException"/> when no sort is available. Point-in-time mode makes
    /// Elasticsearch's implicit <c>_shard_doc</c> tiebreaker explicit so forward and backward
    /// cursors can reverse the complete sort tuple.
    /// </remarks>
    public class SearchAfterQueryBuilder : IElasticQueryBuilder
    {
        private const string ShardDocumentSort = "_shard_doc";

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

            // Search-after paging requires a concrete sort so every cursor can be replayed.
            if (ctx.Options.ShouldUseSearchAfterPaging())
            {
                sortFields ??= new List<SortOptions>();

                var resolver = ctx.GetMappingResolver();
                bool hasIdTiebreaker = IdTiebreakerField.TryEnsure(ctx, sortFields);

                // Live search_after paging with an unstable sort key (e.g. _doc, _score) is only safe
                // within a Point-In-Time: index refreshes and segment merges can invalidate the cursor,
                // silently skipping documents or stopping paging early. Not applicable in PointInTime
                // mode, where a frozen view keeps these sort keys stable.
                bool isLivePaging = ctx.Options.GetSearchAfterPagingMode() is SearchAfterPagingMode.Live;

                // The same ICommandOptions instance is reused across FindResults.NextPageAsync() calls,
                // so BuildAsync runs once per page. Only warn on the first page to avoid flooding logs
                // with the same warning for every page of a long-running paging session.
                bool alreadyWarned = ctx.Options.SafeGetOption<bool>(SearchAfterQueryExtensions.UnstableSortWarnedKey, false);

                // Single pass: resolve each sort's unstable field name (if any) once to check
                // whether a shard-document tiebreaker is already present and for unstable sort
                // keys. SortOptions is a discriminated union: _doc/_score can arrive
                // either as a FieldSort with a literal field name, or as the ES client's typed
                // Doc/Score variants, so both shapes need to be checked.
                bool hasShardDocumentSort = false;
                foreach (var sort in sortFields)
                {
                    string? fieldName;

                    if (sort?.Field?.Field is { } sortField)
                    {
                        fieldName = resolver.GetSortFieldName(sortField);
                        if (fieldName is null)
                            continue;

                        if (String.Equals(fieldName, ShardDocumentSort, StringComparison.Ordinal))
                            hasShardDocumentSort = true;
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

                    if (isLivePaging && !alreadyWarned && UnstableSortFields.Contains(fieldName))
                    {
                        var logger = (ctx.Options.GetElasticIndex() as IHaveLogger)?.Logger ?? NullLogger.Instance;
                        logger.LogWarning("Sorting by {SortField} with Live search_after paging is unstable: {SortField} is not stable across index refreshes or segment merges, so the cursor can become invalid and paging may silently stop early (especially while writing to the index being paged). Sort by a stable, unique field or use SearchAfterPaging(SearchAfterPagingMode.PointInTime).", fieldName, fieldName);
                        ctx.Options.Values.Set(SearchAfterQueryExtensions.UnstableSortWarnedKey, true);
                        alreadyWarned = true;
                    }
                }

                if (isLivePaging && !hasIdTiebreaker && sortFields.Count is 0)
                    throw new QueryValidationException("Live search_after paging requires at least one sortable field. Supply an explicit stable sort or use PointInTime mode.");

                // Elasticsearch implicitly appends _shard_doc ascending to PIT searches. Keep it
                // explicit so SearchBefore can reverse the complete cursor tuple.
                if (!isLivePaging && !hasShardDocumentSort)
                {
                    sortFields.Add(new FieldSort { Field = ShardDocumentSort });
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
