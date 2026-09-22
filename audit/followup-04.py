from pathlib import Path

def replace(path, old, new):
    file = Path(path)
    text = file.read_text()
    assert old in text, (path, old)
    file.write_text(text.replace(old, new))

replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ElasticReindexerTests.cs',
        'using SystemTextJsonSerializer = Foundatio.Serializer.SystemTextJsonSerializer;',
        'using Foundatio.Serializer;\nusing SystemTextJsonSerializer = Foundatio.Serializer.SystemTextJsonSerializer;')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexSafetyTests.cs',
        'public async Task CompletionRead_WhenUnavailable_DoesNotAuthorizeReplay()',
        'public Task CompletionRead_WhenUnavailable_DoesNotAuthorizeReplay()')
replace('tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexSafetyTests.cs',
        '        await Assert.ThrowsAsync<RepositoryException>(() => reindexer.HasCompletionEvidenceAsync(',
        '        return Assert.ThrowsAsync<RepositoryException>(() => reindexer.HasCompletionEvidenceAsync(')
replace('src/Foundatio.Repositories.Elasticsearch/Configuration/Index.cs',
        '<b>Enabling this blocks writes for the duration of the reindex</b>, and the duration grows with index size.',
        '<b>The first pass remains writable. Writes are blocked for the full final copy and verification</b>, so the outage grows with index size.')
for path in ('src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs', 'docs/guide/index-management.md'):
    replace(path, 'created/updated/noop', 'created/updated/deleted/noop')
replace('src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs',
        "Change is detected by the source's maximum sequence number, which advances on inserts, updates, and",
        'Change is detected by the complete per-primary checkpoint vector, which advances on inserts, updates, and')
replace('.agents/skills/foundatio-repositories/references/index-lifecycle.md',
        'created + updated + noops + version_conflicts < total',
        'created + updated + deleted + noops + version_conflicts != total')
path = Path('docs/guide/index-management.md')
text = path.read_text()
start = text.index('A reindex can be interrupted at any point —')
end = text.index('#### When do writes flip', start)
text = text[:start] + '''A terminated process can leave an Elasticsearch task running or a persistent write block in place. Expiry of the 20-minute alias lease is not proof that the old server task stopped.

The reindexer records task dispatch intent before starting a copy. A retry confirms termination of a known task before starting another. Dispatch whose task ID was never recorded remains fenced for operator inspection. Confirmed migration-owned write blocks can be recovered only for the same source UUID and migration; operator-owned and ambiguous blocks are not cleared automatically.

A permitted retry recopies the full source unless the caller explicitly supplied `StartUtc`; it never infers a resume watermark from the destination's newest document. A promoted destination requires matching durable completion evidence. Neither an advanced schema version nor a new `QuiesceSource` flag proves completion, and neither authorizes replay into a live destination.

Higher-level index APIs may skip a version that is already current; that version check is not an integrity audit or a way to repair an unconfirmed migration. Inspect its completion and safety records using the [reindex recovery runbook](/guide/reindex-safety#recovery-and-process-termination).

''' + text[end:]
text = text.replace('a **distributed lock keyed on the alias** (`reindex:audit`) guarantees a given index is never reindexed by two runners at once — even across multiple application instances (pods, workers).', 'a **distributed lock keyed on the alias** (`reindex:audit`) serializes cooperating runners when they share the same distributed lock provider. Durable task state additionally prevents a retry from overlapping a known or ambiguous earlier server task.')
text = text.replace('Throughout, the umbrella alias spans whatever the current partitions are, so reads and writes keep working even while the index is a mix of versions.', 'Throughout, the umbrella alias spans the current partitions, so reads keep working while the index is a mix of versions. In quiesced mode, writes to the partition undergoing its final pass are rejected until cutover.')
text = text.replace('Writes for a period target the **unversioned dated alias**', 'With the default non-quiesced ordering, writes for a period target the **unversioned dated alias**')
path.write_text(text)
