from pathlib import Path
import shutil
import sys

root = Path('src/Foundatio.Repositories.Elasticsearch')
tests = Path('tests/Foundatio.Repositories.Elasticsearch.Tests')
harness = Path('../harness/.audit/sequential327')

def replace(path, before, after):
    text = path.read_text()
    if text.count(before) != 1:
        raise RuntimeError(f'{path}: expected one matching source region, found {text.count(before)}')
    path.write_text(text.replace(before, after))

if sys.argv[1] == 'tests':
    for name in ['ReindexDispatchSafetyTests.cs', 'ReindexRecoveryTests.Retirement.cs']:
        shutil.copyfile(harness / name, tests / name)
    replace(tests / 'ReindexRecoveryTests.cs', 'public sealed class ReindexRecoveryTests(', 'public sealed partial class ReindexRecoveryTests(')
    p = tests / 'Infrastructure/SequenceRequestInvoker.cs'
    replace(p, '    private int _position = -1;', '    private int _position = -1;\n\n    public List<string> Requests { get; } = [];')
    replace(p, '    private InMemoryRequestInvoker Next()\n    {', '    private InMemoryRequestInvoker Next(Endpoint endpoint)\n    {\n        Requests.Add($"{endpoint.Method} {Uri.UnescapeDataString(endpoint.Uri.AbsolutePath)}");')
    replace(p, '=> Next().Request<TResponse>', '=> Next(endpoint).Request<TResponse>')
    replace(p, '=> Next().RequestAsync<TResponse>', '=> Next(endpoint).RequestAsync<TResponse>')
elif sys.argv[1] == 'dispatch':
    shutil.copyfile('../harness/.audit/sequential/SingleAttemptRequest.cs', root / 'Extensions/SingleAttemptRequest.cs')
    p = root / 'Repositories/ReindexTaskLease.cs'
    replace(p, '''        if (status.ApiCallDetails?.HttpStatusCode is 404 || status.IsValidResponse && status.Completed)
            return true;''', '''        // A missing stored result or unavailable owning node cannot establish termination.
        if (status.ApiCallDetails?.HttpStatusCode is 404)
            return false;
        if (status.IsValidResponse && status.Completed)
            return true;''')
    replace(p, 'return status.ApiCallDetails?.HttpStatusCode is 404 || status.IsValidResponse && status.Completed;', 'return status.ApiCallDetails?.HttpStatusCode is not 404 && status.IsValidResponse && status.Completed;')
    p = root / 'Repositories/ElasticReindexer.cs'
    replace(p, 'd.RequestConfiguration(r => r.MaxRetries(0));', 'd.RequestConfiguration(r => SingleAttemptRequest.Configure(_client, r));')
    replace(p, 'var bulkResponse = await _client.Indices.UpdateAliasesAsync(x => x.Actions(aliasActions), cancellationToken).AnyContext();', '''var bulkResponse = await _client.Indices.UpdateAliasesAsync(x => x.Actions(aliasActions)
            .RequestConfiguration(r => SingleAttemptRequest.Configure(_client, r)), cancellationToken).AnyContext();''')
    replace(tests / 'ReindexRecoveryTests.cs', 'Assert.True(status.ApiCallDetails.HttpStatusCode is 404 || status.IsValidResponse && status.Completed);', 'Assert.True(status.ApiCallDetails.HttpStatusCode is not 404 && status.IsValidResponse && status.Completed, status.DebugInformation);')
elif sys.argv[1] == 'retirement':
    p = root / 'Repositories/ElasticReindexer.cs'
    replace(p, '''            await SwitchAliasesAsync(workItem, progressCallbackAsync, 99, cancellationToken).AnyContext();

            // Released explicitly rather than left to the finally: on the success path an index that stays
            // read-only is a failed migration, so it must surface. The finally is the backstop for every other
            // path, where it only logs so it cannot replace the exception that got us there.
            await writeBlock.ReleaseAsync().AnyContext();''', '''            // Persist the retirement boundary before dispatch: a lost alias response may have committed.
            // Never reopen the retired physical source to stale writers, including during exception cleanup.
            await writeBlock.RetainForCutoverAsync(cancellationToken).AnyContext();
            await SwitchAliasesAsync(workItem, progressCallbackAsync, 99, cancellationToken).AnyContext();''')
    replace(p, '    /// <remarks>Cleanup preserves operator blocks. Unconfirmed release or process termination uses durable ownership recovery.</remarks>', '''    /// <remarks>Before cutover intent, cleanup preserves operator blocks. After intent, retained sources stay blocked
    /// even when alias promotion or its response fails. Recovery must reconcile durable intent and actual topology.</remarks>''')
    p = root / 'Repositories/IndexWriteBlock.cs'
    replace(p, '    private bool _releaseAttempted;', '    private bool _releaseAttempted;\n    private bool _retainForCutover;')
    replace(p, '        bool blocked = await IsWriteBlockedAsync(client, workItem.OldIndex, cancellationToken).AnyContext();', '''        if (entry.Phase is "cutover")
            throw new ReindexCompletionUnknownException(workItem.Alias, workItem.OldIndex, workItem.NewIndex,
                "the source block is retained for a committed or ambiguous cutover; reconcile aliases and completion evidence before intervention");
        bool blocked = await IsWriteBlockedAsync(client, workItem.OldIndex, cancellationToken).AnyContext();''')
    needle = '    /// <summary>Removes a block acquired by this instance, with an independent bounded timeout.</summary>'
    replace(p, needle, '''    /// <summary>Retains the source fence before a cutover request whose response may be lost.</summary>
    public async Task RetainForCutoverAsync(CancellationToken cancellationToken)
    {
        // Set the local guard before persisting intent: an uncertain journal write must not enable disposal.
        _retainForCutover = true;
        if (_ownershipId is null)
            return;

        var entry = await ReindexSafetyState.ReadAsync(_client, _ownershipId, cancellationToken).AnyContext();
        if (entry is null || entry.OwnerToken != _ownershipToken || entry.SourceUuid != _sourceUuid || entry.Phase is not "applied")
            throw new RepositoryException("Source block ownership changed; refusing to authorize cutover.");
        string uuid = await ReindexSafetyState.ReadIndexUuidAsync(_client, Index, cancellationToken).AnyContext();
        if (!String.Equals(uuid, _sourceUuid, StringComparison.Ordinal))
            throw new RepositoryException("Source index generation changed; refusing to authorize cutover.");

        await ReindexSafetyState.WriteAsync(_client, _ownershipId, entry with { Phase = "cutover" }, false, cancellationToken).AnyContext();
    }

''' + needle)
    replace(p, '''    private bool TryBeginRelease()
    {
        if (_releaseAttempted)''', '''    private bool TryBeginRelease()
    {
        if (_retainForCutover)
        {
            _logger.LogInformation("Retaining the write block on retired or potentially promoted source {Index}; stale writers must not write to it.", Index);
            return false;
        }

        if (_releaseAttempted)''')
    replace(p, '    /// A pre-existing block is preserved. At most one release attempt is made, including when disposal', '    /// Pre-existing blocks and fences retained for cutover are preserved. At most one release attempt is made, including when disposal')
elif sys.argv[1] == 'docs':
    p=Path('docs/guide/reindex-safety.md')
    replace(p, 'alias switch, owned-block release, durable completion, optional source deletion.', 'durable cutover intent, alias switch, durable completion, optional source deletion. Retained old physical sources stay write-blocked; writes resume through the destination alias, not through the retired source.')
    replace(p, 'A cancellation acknowledgement is followed by a terminal-state check.', 'A cancellation acknowledgement is followed by a terminal-state check. HTTP 404 is unknown, not proof that work stopped; it retains the dispatch fence.')
    replace(p, 'A later attempt can recover a confirmed same-migration block; it preserves an operator block.', 'A later attempt can recover a confirmed same-migration pre-cutover block; it preserves an operator block. Before alias dispatch, the owned record enters `cutover` and automatic unblocking is disabled, including when the response or post-cutover callback fails.')
    replace(p, 'For unknown dispatch, inspect active reindex tasks before clearing an intent.', 'For unknown dispatch, an empty task listing is insufficient: reconcile delayed requests and task lineage before clearing intent. Do not delete or reuse an uncertain destination.')
    replace(p, 'For a stuck source block, establish ownership and generation before removing it.', 'For a stuck pre-cutover source block, establish ownership, generation, task termination, and unchanged source routing before removing it. A `cutover` record requires topology/completion reconciliation and is never automatically unblocked. Retired-source block records may remain after source deletion; do not clear them to permit blind name reuse.')
    replace(p, '## Production prerequisites', '''## Single-attempt dispatch

Async copy submission and alias promotion pin one eligible node for a single effective transport attempt. The supported transport ignores request-local `MaxRetries(0)` alone. Node predicates and explicit forced-node configuration are respected without changing ordinary client retry behavior. Configure proxies and outer retry policies not to replay ambiguous side effects. Task correlation and a journal are not server-side idempotency keys.

## Production prerequisites''')
    p=Path('docs/guide/index-management.md')
    replace(p, '→ reconcile hard deletes → verify → switch aliases → release owned block', '→ reconcile hard deletes → verify → persist cutover intent → switch aliases → retain old source fence')
    replace(p, 'Time-series migration blocks and releases one partition at a time.', 'Time-series migration processes one partition at a time. Writes resume through each upgraded partition alias; a retained old physical partition stays blocked.')
    replace(p, 'Failures before promotion retain the source and attempt bounded block cleanup.', 'Failures before cutover intent retain the source and attempt bounded block cleanup. After intent is persisted, neither success nor failure releases the retired source block automatically.')
    p=Path('.agents/skills/foundatio-repositories/references/index-lifecycle.md')
    s=p.read_text()
    start=s.index('**Recovering from a rolling restart mid-upgrade:**')
    end=s.index('\n\n',start)
    s=s[:start]+'''**Recovering from a rolling restart mid-upgrade:** lease expiry does not prove that the prior server task or cutover stopped. Reconcile durable task/block/completion evidence and physical generations before retrying. Unknown task IDs, task 404, ambiguous cutover intent, and newer ownership forbid inferred cleanup or replay. A known completed task is not proof that an untracked delayed submission cannot arrive. Never manufacture completion from the work-item quiesce flag or blindly recopy into a promoted destination.'''+s[end:]
    s=s.replace('Final copying/reconciliation/verification precede atomic alias promotion; owned blocks are released before completion recording and optional source cleanup. Time-series holds one partition block at a time.', 'Final copying/reconciliation/verification precede durable cutover intent and atomic alias promotion. Sources retained after cutover remain blocked, including after a lost response or observer exception; automatic recovery never unblocks a `cutover` record. Time-series writes resume through each destination alias while retired physical partitions stay fenced.')
    s+='\nAsync copy/alias dispatch uses one eligible node because request-local retry zero alone is ignored by the pinned transport. Task 404 is unknown and cannot clear durable launch intent. These guards do not supply a server-side generation fence or certify lease ownership while Foundatio 13.0.4 remains the dependency.\n'
    p.write_text(s)
else:
    raise RuntimeError('Unknown phase')
