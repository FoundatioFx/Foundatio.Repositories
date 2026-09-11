# Contacts `v33` → `v34` reindex data loss

- **Incident date:** 2026-03-17 (loss window `03:00Z`–`07:00Z`)
- **Reported:** 2026-09 (~6 months later, via a customer-visible application ticket)
- **Confirmed impact:** 6,066 contacts missing from `contacts-production-contact-v34` that still exist in `contacts-production-contact-v33`, counted only among contacts that have an application record. The true population is larger — see [Scoping the damage](#scoping-the-damage).
- **Deployed version:** `v7.18.0-beta4` (tagged 2026-02-27); the next release, `v7.18.0-beta6`, was 2026-04-15.
- **Status:** root causes identified and fixed on `main` (see [Fix status](#fix-status)). Production repair is a separate app-side task specified in [Repair](#repair-specification).

## Summary

A versioned index migration copied contacts from `v33` to `v34`, moved the alias to `v34`, and reported success. It had not finished. Documents written to `v33` during the copy were never carried over, and the migration could not be resumed afterwards because the alias had already moved — the library treats "the alias points at the new version" as "this migration is complete."

Nothing surfaced the failure. The copy's own progress reporting stopped short of 100% but stopped *quietly*; the post-copy document-count comparison found the destination short and responded by **skipping the deletion of the old index rather than raising an error**. The leftover `v33` index was the only externally visible artifact, and it was read as untidiness rather than as an alarm. It was in fact the alarm.

Two independent facts make this a design failure rather than an operational one:

1. **The commit point was in the wrong place.** The alias moved *before* the catch-up copy that was supposed to make the destination complete. So every failure after the flip was unrecoverable by construction.
2. **Every one of those post-flip failures used `return`, not `throw`.** The queued handler saw a completed work item, so the queue never retried, and the caller could not tell a complete migration from an abandoned one.

> A migration that cannot distinguish success from failure is not a migration; it is a coin flip that also deletes your rollback.

## Timeline

| When | What |
|---|---|
| 2025-09-25 | Sample affected contact created (`created_utc` on the doc). Predates the migration by ~6 months, which is what eliminates "these documents never existed." |
| 2026-02-27 | `v7.18.0-beta4` tagged — the build running in production during the incident. |
| 2026-03-07 | `636cf235` (#227) adds `ApplyDateTracking`, auto-setting `UpdatedUtc` on patch operations. **Not yet deployed on 2026-03-17.** First shipped in `v7.18.0-beta6` (2026-04-15). |
| 2026-03-17 `~03:00Z` | Migration starts. First-pass copy begins; `v34` created. |
| 2026-03-17 `03:00Z`–`07:00Z` | Contacts written to `v33` during the copy. Bucketed `updated_utc`: `4 / 2,805 / 2,046 / 1,211` per hour. These are the documents that were lost. |
| 2026-03-17 `~07:00Z` | Alias moves to `v34`. Catch-up pass does not complete. Count comparison finds `v34 < v33`, skips the delete, reports success. `v33` remains on disk. |
| 2026-03 → 2026-09 | `v34` serves all reads. The missing contacts are simply absent. Six months of legitimate writes accumulate in `v34`, which later constrains how the data can safely be repaired. |
| 2026-07 | Migration logs reviewed after an unrelated concern. Nothing found — the run *had* reported completion. (See [M5](#m5--a-copy-could-report-success-with-no-visibility-at-all).) |
| 2026-05-08 | `15c77bf1` (#271) adds the reindex distributed lock and `TryCancelTaskAsync`. First shipped `v7.18.2`. Not present during the incident. |
| 2026-09 | Missing contacts surface through an application ticket. Cross-index join quantifies 6,066. |

## Decoding the evidence

No application logs were available. The RCA rests on the histogram, which is sufficient.

**The epoch keys are the whole story.** The `date_histogram` on the missing documents' `v33.updated_utc`:

| Epoch (ms) | UTC hour | Docs |
|---|---|---|
| `1773716400000` | 2026-03-17 `03:00` | 4 |
| `1773720000000` | 2026-03-17 `04:00` | 2,805 |
| `1773723600000` | 2026-03-17 `05:00` | 2,046 |
| `1773727200000` | 2026-03-17 `06:00` | 1,211 |

Every missing document was last written inside a **single bounded 4-hour window**, with nothing before and nothing after.

**What that rules out.** Elasticsearch `_reindex` scrolls a point-in-time snapshot, so a document present in the source when the pass started is always copied — unless the *source query excluded it* or the *write into the destination failed*. These documents had existed since 2025, so their absence means one of those two things happened. A sharply bounded window with clean edges is the signature of *writes that landed in the old index after the first pass's snapshot and were never caught up*. Per-document write failures (mapping conflicts) would not cluster in time like this; they would cluster by document shape. A resume-watermark skip would not be bounded at all — it would smear missing documents across all of history below a cutoff.

**`v33` still existing is the tell, not a coincidence.** The old index is deleted only after a document-count comparison passes. Its presence is direct evidence that the comparison found the destination short — the code detected the loss and then discarded that finding.

## Failure mechanisms

Ranked by contribution to this incident. Line citations are `v7.18.3` (nearest tag to the deployed build for which the relevant code is unchanged) and pre-fix `main`.

### M1 — the alias flip was the commit point, and every failure after it returned silently

This is the primary cause. The order of operations was: first-pass copy → **flip alias** → catch-up copy → count check → delete old index. The catch-up pass is what makes the destination complete, and it ran *after* the destination started serving reads.

In `v7.18.3`, four separate post-copy failure paths simply return:

```
88:        if (!firstPassResult.Succeeded) return;
123:            if (!secondPassResult.Succeeded) return;
135:                    return;
166:                return;
```

`return`, not `throw`. Consequences, in order:

- `ReindexWorkItemHandler` reports the work item as succeeded, so the queue never retries it.
- The caller has no return value to inspect — `ReindexAsync` returned `void`.
- Progress stops at 92–96 instead of reaching `100 / "Reindex complete"`, but a progress callback that stops early is not an error signal.
- **The migration can never be resumed.** `VersionedIndex.ReindexAsync` decides whether work is needed by comparing the alias's current version to the target version. Once the alias points at `v34`, `GetCurrentVersionAsync()` returns 34, and the migration is considered done permanently.

This matches every observed fact: the bounded loss window (the first-pass duration), "the tasks just don't finish," and `v33` surviving.

### M2 — the catch-up pass keyed on `updated_utc`, which patches did not bump on the deployed build

The catch-up pass selects source documents with `updated_utc >= startTime`. On the build running during the incident, patch operations did not set `UpdatedUtc` — `ApplyDateTracking` (#227, `636cf235`, 2026-03-07) had not shipped yet; it first appeared in `v7.18.0-beta6` on 2026-04-15.

So any contact modified via a patch during the first pass was **invisible to the catch-up pass even when the catch-up pass ran correctly**. This widens the impact from "missing" to "silently stale": a document that was copied early in the first pass and patched later carries pre-migration content in `v34`, and the missing-document join used to size the incident cannot see these at all. Residual exposure today: any consumer-supplied `ScriptPatch` or update-by-query script that doesn't touch `updated_utc`.

### M3 — the resume watermark could skip documents that were never copied

Not the cause of *this* incident (the bounded window rules it out), but a live silent-loss path in the same code, and the more dangerous of the two because it is unbounded.

Progress was inferred by reading `max(updated_utc)` from the **destination** and copying only documents at or after it (`GetResumeStartingPointAsync`, `v7.18.3` line 461). `_reindex` copies in scroll/doc order, **not timestamp order**, so a partially populated destination's maximum timestamp is not a watermark — it is close to the global maximum. An interrupted first pass therefore makes the next attempt match almost nothing, report success, and flip the alias.

`#271` made this *more* reachable: `TryCancelTaskAsync` means an abandoned status poll now actively cancels the server-side copy that previously would have run to completion on its own. A reliability improvement turned a recoverable state into a lossy one. Distinguishing signature: missing documents smeared across all history below a cutoff, rather than a bounded window.

### M4 — the destination was never verified before the flip, and a shortfall afterwards was not an error

There was no pre-flip verification at all. The post-flip `Old Docs / New Docs` comparison existed but only gated the deletion of the old index: when `new < old` it skipped the delete and still reported `100 / "Reindex complete"`. No warning, no error, no exception — the only artifact was the leftover index. The check detected this incident and threw the finding away.

### M5 — a copy could report success with no visibility at all

Per-pass counters came from deserializing the task-status body on a single poll. If that body did not bind, the pass reported `Total: 0 Completed: 0 Failures: 0` **with success**. `version_conflicts` was logged but never treated as a defect, and `failures` was only read from the final poll.

This is why the July log review found nothing: the run genuinely had reported completion, and there was no persisted record of the task's own accounting to contradict it.

### M6 — deletes are resurrected, which constrains repair

Documents hard-deleted from the source after the first-pass snapshot are copied into the destination anyway, and no pass propagates deletes. This did not cause the loss, but it is the reason a naive "copy `v33` into `v34`" repair is unsafe: it would resurrect contacts that were legitimately deleted or merged. See [Repair](#repair-specification).

### M7 — concurrent reindexes were possible during the incident

Before `#271` (`v7.18.2`, 2026-05-08), `VersionedIndex.ReindexAsync` took no lock while `ReindexWorkItemHandler` did, so a direct `ConfigureIndexesAsync`/`ReindexAsync` call could run alongside the queued work item. The second runner computes an M3 watermark from the first runner's in-flight destination, finishes in seconds, flips the alias early, and both then copy stale content over live writes. Live on 2026-03-17; fixed by the time of writing.

## The 7.x gap

Upgrading 7.x → 8.x would **not** have prevented this incident. The mechanisms that caused it were present in both branches.

| Mechanism | In `v7.18.3` | In pre-fix `main` (8.x) | Fixed by upgrading? |
|---|---|---|---|
| M1 flip-before-catch-up + silent `return` | yes | yes | **no** |
| M2 patches not bumping `updated_utc` | fixed in `v7.18.0` | fixed | n/a (already fixed) |
| M3 unsound resume watermark | yes | yes | **no** |
| M4 no pre-flip verification | yes | yes | **no** |
| M5 zero-visibility success | yes | yes | **no** |
| M6 deletes resurrected | yes | yes | **no** |
| M7 concurrent reindex | fixed in `v7.18.2` | fixed | n/a (already fixed) |

Genuinely 8.x-only improvements, none of which address the loss class:

- **Status-poll backoff (#293).** `v7.18.3` does a bare `continue` on a failed `GET _tasks/<id>` with no delay and abandons after `MAX_STATUS_FAILS = 10` (line 29). Eleven fast failures — e.g. 429s under indexing pressure — abandon the pass, and since `#271` also cancel the server-side task.
- **Alias-flip response validation.** `v7.18.3` calls `BulkAliasAsync` (line 101) and never inspects the response. A failed flip falls through to the catch-up pass and the count comparison, and could delete the old index while the alias still pointed at it.
- **Throttling knobs** (`ReindexBatchSize`, `ReindexRequestsPerSecond`) and reindex argument validation.

## Forensic runbook

No application logs are needed. The reindex was launched with `wait_for_completion=false`, which means **Elasticsearch persisted every task result into the `.tasks` index, and those records do not expire.** The March migration's own accounting is still on the cluster.

### 1. Recover the migration's task records

```
GET .tasks/_search
{
  "size": 100,
  "query": {
    "bool": {
      "filter": [
        { "term":  { "task.action": "indices:data/write/reindex" } },
        { "range": { "task.start_time_in_millis": { "gte": 1773705600000, "lt": 1773792000000 } } }
      ]
    }
  },
  "sort": [ { "task.start_time_in_millis": "asc" } ]
}
```

Each hit gives:

- `task.description` — source index, destination index, **and the source query**. This is the decisive field: it shows whether a pass was narrowed by a range filter, which confirms or eliminates M3 directly.
- `task.status` — `total`, `created`, `updated`, `noops`, `version_conflicts`, `batches`, `retries`, `throttled_millis`. If `created + updated + noops + version_conflicts < total`, the copy did not account for every document it matched.
- `response.failures` — per-document write failures.
- `completed`, `canceled`, and the start/duration — how many attempts there were, whether the catch-up pass ran at all, and whether anything was cancelled (M3 via `#271`).

### 2. Pin the migration window

```
GET _cat/indices/contacts-production-contact*?v&h=index,docs.count,creation.date.string&s=creation.date
```

`v34`'s creation date is the first-pass start (`T0`). **If it is ~`2026-03-17T03:00Z`, the missing-document window is exactly the first-pass window, and M1 is confirmed.** This also reveals whether `contacts-production-contact-v34-error` exists, which would hold per-document copy failures.

### 3. Distinguish M1 from M3

```
GET contacts-production-contact-v33/_search
{
  "size": 0,
  "aggs": {
    "over_time": { "date_histogram": { "field": "updated_utc", "calendar_interval": "day" } },
    "oldest":    { "min": { "field": "updated_utc" } },
    "newest":    { "max": { "field": "updated_utc" } }
  }
}
```

Run against both `v33` and `v34` and compare shapes:

- **Bounded gap** confined to the migration window → M1 (writes during the copy were never caught up).
- **Smeared gap** below a cutoff, across all history → M3 (watermark skipped documents that were never copied).

### 4. Quantify stale documents (M2) — likely the larger number

Sample documents present in **both** indexes and compare `updated_utc`. Any document whose `v34.updated_utc` is *older* than its `v33.updated_utc` was copied and then modified in `v33` without the catch-up pass picking it up. The missing-document join cannot see these at all, so this is unmeasured damage.

### 5. Establish the true suspect population

Anchor `T0` (from step 2) and `T_flip` (`T0` + first-pass duration from step 1), then:

```
GET contacts-production-contact-v33/_count
{ "query": { "range": { "updated_utc": { "gte": "2026-03-17T03:00:00Z" } } } }
```

That is every document at risk. **6,066 is only the subset that both has an application record and is entirely missing** — it is a floor, not the impact.

## Scoping the damage

The reported 6,066 undercounts on two axes:

1. **Entity scope.** The join filtered to contacts having an application record. That was the right instinct for triage — an activity record is good evidence a contact is real rather than merged away — but contacts without applications were equally exposed.
2. **Damage type.** It counted only *missing* documents. Per M2, documents that were copied and then patched during the first pass are *present but stale*. They are invisible to a missing-only query and require the step-4 comparison to find.

## Repair specification

Do not bulk-copy `v33` over `v34`. Six months of legitimate writes live in `v34`, and `v33` contains contacts that were later deleted or merged (M6) — a blind copy resurrects them and clobbers current data.

Classify every candidate document first, using `T_flip` as the pivot:

| State | Action | Why |
|---|---|---|
| Missing from `v34` | Copy from `v33` with `op_type: create` | `create` cannot overwrite anything, so it is safe by construction. Must be filtered against an exclusion set of legitimately deleted/merged contacts. |
| In both, `v34.updated_utc < v33.updated_utc` **and** `v34.updated_utc < T_flip` | Safe to overwrite from `v33` | `v34` was never written after the cutover, so `v33` is strictly newer. |
| In both, `v34.updated_utc >= T_flip` | **Never auto-overwrite** | Live document with post-cutover writes. Needs field-level reconciliation, if anything. |

Repair copy:

```
POST _reindex
{
  "source": {
    "index": "contacts-production-contact-v33",
    "query": { "range": { "updated_utc": { "gte": "2026-03-17T03:00:00Z" } } }
  },
  "dest": { "index": "contacts-production-contact-v34", "op_type": "create" },
  "conflicts": "proceed"
}
```

`op_type: create` plus `conflicts: proceed` is the safety belt: existing documents are reported as conflicts and left untouched, so this pass can only add what is missing. Run it against a snapshot restore first and diff the result.

**Take a snapshot before any repair.** `v33` is the only remaining copy of the lost documents.

## Fix status

All mechanisms in this RCA that were live on `main` have been fixed. Each fix has a regression test that was verified to fail before the change.

| Mechanism | Fix |
|---|---|
| M1 (silent post-flip `return`) | Every incomplete outcome now throws `ReindexIncompleteException`. Each copy pass returns a `ReindexOutcome` (`Completed`/`NotStarted`/`Abandoned`/`Failed`) plus a reason, instead of a single `bool` that conflated "never started," "gave up waiting," and "copied some documents." `ElasticConfiguration.ReindexAsync` aggregates per-index failures and throws. |
| M1 (unresumable after flip) | The pre-cutover guarantee is now explicit: the first pass must report that it finished *and* account for every document it matched (`created + updated + noops + version_conflicts >= total`) before the alias moves. |
| M2 | `ApplyDateTracking` (#227) shipped in `v7.18.0`; sources are now also refreshed before every copy pass, since Elasticsearch only makes writes searchable on refresh. |
| M3 | **The watermark is deleted.** A pass is only ever narrowed by an *explicit* start time (the caller's `StartUtc`, or the copy's own start timestamp for the catch-up pass) — never by inspecting the destination. A retry recopies from the beginning, which converges because reindex writes by document id. |
| M4 | Count verification now runs on every reindex rather than only inside the delete branch. It is deliberately warn-only and non-authoritative, because it runs post-cutover where hard deletes through the alias and `ctx.op` script drops make it unreliable; the race-free check is the per-pass accounting above. |
| M5 | The raw task-status body is now requested per-request and **fails closed** when it is missing or unparseable, instead of reporting `Total: 0` with success. Per-document failures are written to a searchable `-error` index (`dynamic: strict`, typed `ReindexFailure`) instead of being unqueryable. `version_conflicts` now feed progress accounting and the stall watchdog. |
| M6 | Documented as a repair-time hazard (above). Delete propagation is out of scope for a copy-based migration. |
| M7 | Fixed in `v7.18.2` (#271). Additionally, alias *maintenance* now takes the same `reindex:{alias}` lock, so periodic maintenance can no longer revert a cutover mid-flight. |
| Cross-cutting | `CancellationToken` plumbed through; cancellation unwinds before the cutover, so it can never promote a partial index. Alias metadata (filter/routing/`is_write_index`/`is_hidden`) is preserved across the flip. The queued handler path was brought to parity with the direct path. |

### Known limitation

Index locks are only as distributed as their backing cache. When neither a lock provider nor a cache client is configured, the library falls back to an in-memory cache, which serializes **within one process only** — two instances would each believe they hold the reindex lock. The constructor now logs a warning in that case. Configure a distributed cache before running more than one instance.

## What this incident says about the design

Three failures of principle, each of which independently would have prevented six months of silent loss:

1. **The commit point must come after the guarantee, not before it.** The alias moved before the destination was known complete. Anything sequenced after an irreversible step must be assumed not to run.
2. **A detected problem that is not propagated is worse than an undetected one.** The count comparison *found* this incident and responded by skipping a cleanup step. The code knew.
3. **Absence of an error is not evidence of success.** The only signal was a leftover index — an artifact that looks like untidiness. Success must be positively asserted, which is why the fix throws rather than logs.

The follow-on operational gap is verification: index migrations were deployed without any post-deployment check that the destination matched the source. The `.tasks` records needed to do that check existed the whole time and were never read.
