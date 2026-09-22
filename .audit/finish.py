from pathlib import Path
import sys


def replace(path, old, new, count=1):
    file = Path(path)
    text = file.read_text(encoding='utf-8-sig')
    assert text.count(old) == count, (path, old[:90], text.count(old), count)
    bom = file.read_bytes().startswith(b'\xef\xbb\xbf')
    file.write_text(text.replace(old, new), encoding='utf-8-sig' if bom else 'utf-8')


root = 'src/Foundatio.Repositories.Elasticsearch/'
repository = root + 'Repositories/ElasticReadOnlyRepositoryBase.cs'
if sys.argv[1] == 'scalar':
    replace(repository, '''                    if (response.IsValidResponse && pagingStrategy is PagingStrategy.SearchAfterLive or PagingStrategy.SearchAfterPointInTime &&
                        (response.TimedOut || response.Shards?.Failed > 0))
                    {
                        string failures = String.Join("; ", response.Shards?.Failures?.Select(failure => failure.Reason.Reason) ?? []);
                        throw new DocumentException($"Incomplete cursor search: timed_out={response.TimedOut}, failed shards={response.Shards?.Failed}. {failures}", response.OriginalException());
                    }''', '''                    ThrowIfIncompleteCursorSearch(response, pagingStrategy is PagingStrategy.SearchAfterLive or PagingStrategy.SearchAfterPointInTime);''')
    replace(repository, 'ThrowIfPointInTimePagingIsUnsupported(', 'ValidateScalarPagingOptions(', 8)
    replace(repository, '        if (options.ShouldUseSearchAfterPagingPointInTime())\n            throw new QueryValidationException($"{operation}', '        if (GetPagingStrategy(options) is PagingStrategy.SearchAfterPointInTime)\n            throw new QueryValidationException($"{operation}')
    for size in (1, 0):
        replace(repository, f'        searchDescriptor.Size({size});', f'''        searchDescriptor.Size({size});
        if (options.ShouldUseSearchAfterPaging())
            searchDescriptor.AllowPartialSearchResults(false);''')
    replace(repository, '        searchDescriptor.DocvalueFields(new FieldAndFormat[] { new() { Field = _idField!.Value } });', '''        searchDescriptor.DocvalueFields(new FieldAndFormat[] { new() { Field = _idField!.Value } });
        if (options.ShouldUseSearchAfterPaging())
            searchDescriptor.AllowPartialSearchResults(false);''')
    replace(repository, '        result = response.Hits.Select(h => h.ToFindHit()).ToList();', '''        ThrowIfIncompleteCursorSearch(response, options.ShouldUseSearchAfterPaging());
        result = response.Hits.Select(h => h.ToFindHit()).ToList();''')
    replace(repository, '''            _logger.LogRequest(response, options.GetQueryLogLevel());
            result = response.ToCountResult(options, ElasticIndex.Configuration.Serializer, _logger);
        }

        await OnAfterQueryAsync''', '''            _logger.LogRequest(response, options.GetQueryLogLevel());
            ThrowIfIncompleteCursorSearch(response, options.ShouldUseSearchAfterPaging());
            result = response.ToCountResult(options, ElasticIndex.Configuration.Serializer, _logger);
        }

        await OnAfterQueryAsync''')
    replace(repository, '        return response.Total > 0;', '''        ThrowIfIncompleteCursorSearch(response, options.ShouldUseSearchAfterPaging());
        return response.Total > 0;''')
    replace(repository, '    private static void ValidateScalarPagingOptions(ICommandOptions options, string operation)', '''    private static void ThrowIfIncompleteCursorSearch<TDocument>(SearchResponse<TDocument> response, bool useSearchAfter)
    {
        if (!useSearchAfter || !response.IsValidResponse || (!response.TimedOut && response.Shards?.Failed is not > 0))
            return;

        string failures = String.Join("; ", response.Shards?.Failures?.Select(failure => failure.Reason.Reason) ?? []);
        throw new DocumentException($"Incomplete cursor search: timed_out={response.TimedOut}, failed shards={response.Shards?.Failed}. {failures}", response.OriginalException());
    }

    private static void ValidateScalarPagingOptions(ICommandOptions options, string operation)''')
elif sys.argv[1] == 'docs':
    replace(root + 'Configuration/Index.cs', '    public bool HasSortableIdField { get; protected set; } = true;', '    /// <inheritdoc />\n    public bool HasSortableIdField { get; protected set; } = true;')
    replace(root + 'Queries/Builders/SearchAfterQueryBuilder.cs', '        /// Resetting or replacing a session invalidates its existing continuations, including changes in BeforeQuery.', '''        /// Resetting or replacing a Live or point-in-time session invalidates its existing continuations,
        /// including changes in BeforeQuery or AfterQuery. Reapplying the current mode preserves the session.
        /// Command options and result objects must not be shared by concurrent traversals.''')
    path = 'docs/guide/querying.md'
    replace(path, '''A continuation belongs to its original PIT session: resetting or replacing that session invalidates
its results even if a new `FindAsync` call opens another PIT with the same options. This is checked
''', '''A continuation belongs to its original Live or PIT session: disabling search-after, switching modes,
or disabling and re-enabling the same mode invalidates the old results, even if a new `FindAsync`
call starts a replacement traversal with the same options. Reapplying the current mode without a
reset preserves the active session. Resetting from `AfterQuery` also invalidates the returned page's
continuation. This is checked
''')
    replace(path, 'previous mode. A PIT continuation cannot switch modes or replace its session in that handler.', 'previous mode. Neither a Live nor a PIT continuation can switch modes or replace its session in that handler.')
    replace(path, '''ending a traversal or skipping documents.
''', '''ending a traversal or skipping documents. Live cursor queries through `FindOneAsync`, `CountAsync`,
and query-based `ExistsAsync` enforce the same complete-response requirement and reject combinations
with snapshot/scroll or async paging before submitting a search.

A paging session is not a concurrency primitive. Use separate command options and results for each
independent traversal, and await each `NextPageAsync()` before requesting another page. Keep the query,
sort definitions, and target indexes unchanged while continuing. Live paging is not a snapshot: changes
to the matching set or sort values can still affect later pages; use PIT for a consistent view.
''')
    replace(path, '''> The id tiebreaker is skipped entirely for models that don't implement `IIdentity` (there is no id
> to sort by) and for indexes managed outside this library (see
''', '''> The id tiebreaker is skipped entirely for models that don't implement `IIdentity` (there is no id
> to sort by) and for indexes that explicitly set `HasSortableIdField = false` (see
''')
    path = '.agents/skills/foundatio-repositories/SKILL.md'
    replace(path, 'Results remain bound to their original PIT session across hooks and cannot attach to a replacement session.', 'Results remain bound to their original Live or PIT session across hooks and cannot attach to a replacement session. Reapplying the same mode preserves identity; disabling and re-enabling it does not. Do not share mutable options/results between concurrent traversals.')
    replace(path, 'Scalar Live cursors bypass caches after `BeforeQuery`.', 'Scalar Live cursors bypass caches after `BeforeQuery`, reject incomplete search responses, and reject snapshot/async paging combinations.')
    path = '.agents/skills/foundatio-repositories/references/patterns.md'
    replace(path, 'Continuations validate the originating PIT session after `BeforeQuery` and cannot attach to replacement sessions.', 'Continuations validate the originating Live or PIT session before cursor mutation and after `BeforeQuery`; resetting in `AfterQuery` invalidates the returned continuation too. Separate mutable options/results per traversal and await each next page; query, sorts, and targets must remain unchanged.')
    replace(path, '`FindOneAsync` and `CountAsync` run `BeforeQuery` before cache decisions and bypass both cache reads and writes for Live cursors.', '`FindOneAsync` and `CountAsync` run `BeforeQuery` before cache decisions and bypass both cache reads and writes for Live cursors. Those operations and query-based `ExistsAsync` reject incomplete Live responses and incompatible snapshot/async modes.')
else:
    raise ValueError(sys.argv[1])
