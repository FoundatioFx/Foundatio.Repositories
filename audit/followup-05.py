from pathlib import Path

def replace(path, old, new):
    file = Path(path)
    text = file.read_text()
    assert old in text, (path, old)
    file.write_text(text.replace(old, new))

replace('src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs',
        '$"/{Uri.EscapeDataString(index)}/_mget?_source=false",',
        '$"/{Uri.EscapeDataString(index)}/_mget?_source=false&stored_fields=_routing",')
replace('src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs',
        '    /// <summary>Only an explicit, error-free absence response authorizes deleting a destination key.</summary>',
'''    /// <summary>Only an explicit, error-free absence response authorizes deleting a destination key.</summary>
    /// <remarks>
    /// Request routing explicitly: with source retrieval disabled and no stored fields requested,
    /// Elasticsearch can skip its stored-field loader and omit routing even for a routed document.
    /// That omission must not be mistaken for a change of document identity.
    /// </remarks>''')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs',
        "        // Block the completion write by taking the record's name as a closed index, so writes to it fail.",
        '        // Keep completion reads available so this exercises the final write, not preflight read failure.')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs',
'''        await _client.Indices.CloseAsync(completionIndex, TestCancellationToken);
        await using AsyncDisposableAction cleanup = new(async () =>
        {
            await _client.Indices.OpenAsync(completionIndex, TestCancellationToken);
            await _client.Indices.DeleteAsync(completionIndex, TestCancellationToken);
        });''',
'''        var block = await _client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.PUT,
            $"/{completionIndex}/_settings", PostData.String("""{"index.blocks.write":true}"""), TestCancellationToken);
        Assert.True(block.ApiCallDetails.HasSuccessfulStatusCode, block.Body);
        await using AsyncDisposableAction cleanup = new(async () =>
        {
            await _client.Indices.DeleteAsync(completionIndex, TestCancellationToken);
        });''')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexTests.cs',
'''        Assert.Contains("completion could not be recorded", exception.Reason);
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);''',
'''        Assert.Contains("completion could not be recorded", exception.Reason);
        Assert.Equal(5, (await _client.CountAsync<Identity>(c => c.Indices(version2Index.VersionedName), TestCancellationToken)).Count);
        Assert.False(await reindexer.HasCompletionEvidenceAsync(workItem, TestCancellationToken));
        Assert.True((await _client.Indices.ExistsAsync(version1Index.VersionedName, cancellationToken: TestCancellationToken)).Exists);''')
replace('docs/guide/index-management.md', '#recovery-and-process-termination', '#recovery-procedure')
replace('src/Foundatio.Repositories.Elasticsearch/Repositories/IndexWriteBlock.cs',
        'Blocked writes to index {Index} for the duration of the reindex.',
        'Blocked writes to index {Index} for final reconciliation and cutover.')
replace('src/Foundatio.Repositories.Elasticsearch/Configuration/DailyIndex.cs',
'''    /// Deleting expired partitions is deliberately <em>not</em> gated on the lock. It is unbounded, so holding
    /// the un-renewed lock across it would be unsafe, and it cannot collide with a reindex because a reindex
    /// already skips partitions past their expiration date. Skipping it during a reindex would also stall
    /// retention at the exact moment a reindex has the source and destination on disk at once.''',
'''    /// Expired-partition deletion is not gated on this alias-maintenance lock. Reindexing skips partitions
    /// already expired when enumerated, but a partition can expire during a long copy. Coordinate retention
    /// with migrations near an expiration boundary; the alias lock alone does not protect against deletion.''')
