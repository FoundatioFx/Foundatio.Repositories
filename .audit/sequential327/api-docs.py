from pathlib import Path

def replace(path, old, new):
    p=Path(path)
    s=p.read_text()
    if s.count(old)!=1:
        raise RuntimeError(f'Expected one match in {path}, got {s.count(old)}')
    p.write_text(s.replace(old,new))

replace('src/Foundatio.Repositories.Elasticsearch/Configuration/Index.cs', '''    /// For a time-series index the block is applied per partition and released before the next one starts, so the
    /// write outage covers a single partition at a time rather than the whole migration.''', '''    /// Time-series migration processes one partition at a time. Writes resume through the destination alias;
    /// retained old physical partitions remain blocked so stale writers cannot acknowledge stranded writes.''')
replace('src/Foundatio.Repositories.Elasticsearch/Configuration/DailyIndex.cs', '''                // Per partition: the block is applied when this work item runs and released before the next
                // iteration, so the write outage covers one partition at a time instead of spanning the whole
                // multi-partition migration.''', '''                // Process one active partition at a time. Its writes resume through the promoted alias;
                // an old physical partition retained after cutover remains blocked against stale writers.''')
replace('src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItem.cs', '''    /// retain and retry rejected writes through aliases. A full copy is required: StartUtc is unsupported.''', '''    /// retain and retry rejected writes through aliases. Retained old physical sources remain blocked after
    /// cutover intent, including when the alias response is lost. A full copy is required: StartUtc is unsupported.''')
replace('docs/guide/troubleshooting.md', 'A fresh attempt can recover a confirmed migration-owned block for the same physical generation.', 'A fresh attempt can recover a confirmed pre-cutover migration-owned block for the same physical generation. A `cutover` intent is not automatically unblocked: retained old sources must reject stale physical-name writers even after successful promotion.')
replace('docs/guide/troubleshooting.md', 'Explicit release failures propagate on the success path; disposal during another failure logs without masking the original error.', 'Before cutover intent, explicit release failures propagate and disposal during another failure logs without masking the original error. After cutover intent, disposal deliberately retains the retired source fence.')
