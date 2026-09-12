# Ledger: refuse automatic cutover when the migration guarantee cannot be satisfied

Status: **implemented (narrow scope)** — approved 2026-09-11, implemented same day
Scope gate: clarifies existing plan sections 4c/4d and release blocker B1. Does **not** expand scope.

## What was implemented

Approved scope was the narrow pre-Switch fix only: **no** Quiesce/write-block option, no phase reordering, no
journal, no cache redesign, no full scans.

- `PlanCatchUpAsync` runs **before** the copy and classifies the migration. Only the "no timestamp field **and**
  sampled id is not an ObjectId" case is treated as unable to catch up. It records the source's highest
  `_seq_no` at that point.
- `EnsureCatchUpPossibleAsync` runs **before** `SwitchAliasesAsync`. It re-reads the source's `_seq_no` and
  throws `ReindexIncompleteException` if it advanced, if it cannot be read, or if it could not be read at plan
  time. Refusal leaves the alias on the source and retains the source.
- Change detection uses `_seq_no` from shard-level stats, chosen because it advances on inserts, updates **and**
  deletes. Verified on a live cluster that a `max` aggregation on `_seq_no` does *not* see deletes (reported 4
  while the shard reported 5), which is why stats are used instead. Cost is a per-shard stats read, independent
  of index size — no scan was added.
- The sampled id is used only in the negative direction (recognizing a definitely-unsliceable source), never as
  proof that catch-up will succeed, since ids need not be homogeneous.

**Explicitly not claimed:** this prevents one unsafe promotion path. It does not make migrations lossless. The
alias still moves before the catch-up pass for models that *can* catch up, so writes in that window remain a
live-write race, and the `_seq_no` gate is a refusal to promote rather than a write barrier. Documented as
"Remaining limitations" in `docs/guide/index-management.md`.

## Behavior

Today a source with no timestamp field whose ids are not ObjectIds **skips the catch-up pass entirely** and
still reports `100 / "Reindex complete"`. Documents written to the source during the copy are lost, with only a
`LogWarning` (`ElasticReindexer.cs`, `SampleIdStatus.Found` arm of the no-timestamp branch).

The approved policy is **not** "throw when ids are custom". Custom ids and models without date fields are
supported configurations and must stay supported. The refusal condition is narrower:

> Refuse automatic cutover when the **requested guarantee cannot be satisfied** for a **live** source — i.e.
> concurrent writes cannot be accounted for. Do not refuse merely because a timestamp field or ObjectId ids are
> absent.

Consequences of that framing:

- **A statically-sourced (frozen) migration must not require a timestamp or ObjectId.** Needing one is an
  artifact of the legacy catch-up implementation, not a real correctness requirement. The date-independent
  procedure already specified in plan section 4c is **Quiesce**: apply an explicit source write block via the
  add-write-block API (which accounts for the block across shards and waits for in-flight writes), refresh, then
  replay until a pass reports zero created and zero updated. Convergence is meaningful *only* because the source
  cannot change. This needs no timestamps and no ObjectIds.
- **Field availability is not proof.** Neither the presence of a timestamp field nor a single sampled ObjectId
  establishes that catch-up is sound. The decision follows the actual algorithm and the supported write
  behavior. (A sampled id is one document; ids are not required to be homogeneous.)
- **Detection belongs before the expensive copy** where it can be determined, and the safety requirement is
  **always** enforced before `Switch`.
- **Enforcement is a refusal to promote, not a post-switch exception.** Replacing a post-switch warning with a
  post-switch throw is not prevention — the alias has already moved. The source is retained either way.

## Weaker mode

An explicit best-effort/live path is acceptable **only** as a clearly weaker contract, and must reuse the
operation-mode/result design from plan sections 4c/4d rather than introducing an overlapping "ignore safety"
flag. It must:

- Report that the copy finished but that **live consistency was not verified** — a distinguishable outcome, not
  the same "complete/verified" vocabulary as strict mode.
- **Not** bypass strict-mode verification, and not suppress unrelated failures.
- **Retain the old index** rather than auto-deleting the recovery source.

## Baseline evidence

`ElasticReindexer.cs` no-timestamp branch: `SampleIdStatus.Found` with a non-ObjectId id logs a warning and
falls through; `progressCallbackAsync(100, "Reindex complete")` still fires and the alias has already been
switched by that point. No test covers it.

## Tests (written first, RED before the fix, GREEN after)

Commands (audit cluster on `:9201`):

```bash
dotnet test tests/Foundatio.Repositories.Elasticsearch.Tests/Foundatio.Repositories.Elasticsearch.Tests.csproj \
  --filter "FullyQualifiedName~WithStaticSourceAndCustomIds|FullyQualifiedName~BeforeCutoverCannotBeCaughtUp"
dotnet test tests/Foundatio.Repositories.Elasticsearch.Tests/Foundatio.Repositories.Elasticsearch.Tests.csproj \
  --filter "FullyQualifiedName~QueuedReindex"
```

1. **Characterization, kept passing:** `ReindexAsync_WithStaticSourceAndCustomIds_CopiesEveryDocument` — a
   static, date-free, custom-id copy succeeds. Pins that the refusal was not implemented too broadly. Confirmed
   still GREEN with the guard disabled, so it does not depend on it.
2. **False success (insert):** `ReindexAsync_WhenDocumentWrittenBeforeCutoverCannotBeCaughtUp_DoesNotReportSuccess`.
   RED before: *"The document written before cutover is missing from the destination, yet the migration promoted
   it and reported success (exception: none)."*
3. **Same-count variant (modify):** `ReindexAsync_WhenDocumentModifiedBeforeCutoverCannotBeCaughtUp_DoesNotReportSuccess`.
   RED before: *"The old index was deleted even though the destination never received the pre-cutover
   modification, destroying the only copy of it."* Counts match in this case, so cardinality checks cannot catch
   it — which is also why `_seq_no` was chosen over counts.
4. **Refusal is complete:** both tests assert `ReindexIncompleteException`, alias still on v1, and that progress
   never reported 100.
5. **Queued path:** `QueuedReindex_WhenCatchUpImpossibleAndSourceChanged_FailsWithoutPromoting` — surfaced as a
   failure rather than an acknowledged success, with nothing promoted or deleted.
6. **Regression:** 92/92 across `ReindexTests`, `ElasticReindexerTests`, `MigrationTests`,
   `DisposableClusterGuardTests`.
7. **Fix-disabled check:** short-circuiting `EnsureCatchUpPossibleAsync` reproduces both failures at the intended
   assertions, confirming the tests are actually driven by the guard.

Both mutation tests treat a *rejected* pre-cutover write as a pass, since a source write barrier is a valid
implementation of the same guarantee; asserting the write succeeded would fail against a correct future
implementation.

## Files and public contracts affected

- `src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs` — phase enforcement before Switch.
- `src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItem.cs` — mode selection (serialized shape: see
  compatibility below).
- `src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItemHandler.cs` — unsupported-configuration
  failures must not retry forever.

## Compatibility impact (to carry into release notes)

- **Default behavior changes:** a migration whose source is written to during the copy, and which has no way to
  catch up (no timestamp field, non-ObjectId ids), now throws `ReindexIncompleteException` instead of promoting
  and reporting success. The old index is retained, so the refusal is recoverable — retry with writes stopped, or
  add `IHaveDates`/ObjectId ids.
- **Unchanged:** static sources with custom ids and date-free models still copy and promote normally. Explicit
  `StartUtc`/`TimestampField` inputs and low-level incremental copy behavior are untouched.
- **Serialized work items:** no new field was added to `ReindexWorkItem`, so already-enqueued items are
  unaffected and no default-value compatibility question arises. This was a deliberate consequence of deciding
  the condition from the source's own state rather than from a new opt-in flag.
- **Extra requests per reindex:** at most two shard-level stats reads, and only for the no-timestamp,
  non-ObjectId case. No additional cost for the common path.

## Verified findings that affect implementation

Both established empirically rather than assumed, per the lesson from the previous audit.

**1. The dedicated add-block API exists on the server and has the semantics the plan relies on.**

```console
$ curl -X PUT "127.0.0.1:9201/blocktest/_block/write"
{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"blocktest","blocked":true}]}

$ curl -X PUT "127.0.0.1:9201/blocktest/_doc/2" -d '{"a":2}'
{"error":{"root_cause":[{"type":"cluster_block_exception",
 "reason":"index [blocktest] blocked by: [FORBIDDEN/8/index write (api)];"}]},"status":403}
```

`shards_acknowledged: true` plus per-index `blocked: true` is the cross-shard accounting the guarantee needs,
and the resulting block is distinguishable from a settings change (`index write (api)`, `FORBIDDEN/8`). Reads
are unaffected, so the copy can still scroll the source.

**2. The typed .NET client (`Elastic.Clients.Elasticsearch` 8.19.25) does not expose it.** A full scan of the
assembly for `block` yields only `IndexSettingBlocks`/`IndexSettingBlocksDescriptor` — the settings-based
`index.blocks.write` path, which plan section 4c explicitly forbids substituting — plus unrelated Azure
repository names (`PutBlock`, `PutBlockList`). There is no `AddBlockAsync`/`Indices.AddBlock`.

Implication: Quiesce must issue the request through the low-level transport rather than a typed helper. That is
acceptable — the semantics that matter are server-side — but it means the request and its response validation
(`acknowledged`, `shards_acknowledged`, and per-index `blocked`) are hand-rolled and must be asserted by tests
rather than trusted to the client. Note also that a block **persists**: any path that does not delete the source
must remove it again, including on failure.

## Implementation shape (the reason this is not a guard clause)

The current flow switches the alias **before** the catch-up pass runs:

```text
Copy (0-90) -> EnsureCopyCompleted -> SwitchAliasesAsync -> catch-up (92-96) -> verify counts -> delete old
```

Satisfying "always enforce the safety requirement before Switch" therefore cannot be done by adding a check to
the existing no-timestamp branch — that branch runs *after* the cutover, which is exactly the post-switch throw
this entry rejects. It requires moving the commit point, i.e. the plan's section 4c ordering:

```text
Prepare -> Copy -> Quiesce -> Reconcile -> Verify -> Switch -> CacheHandoff -> Complete
```

Once Reconcile runs against a write-blocked source, the timestamp/ObjectId question disappears for strict mode:
convergence on "zero created, zero updated" is meaningful without any ordering field, which is what makes the
procedure date-independent. The no-timestamp branch's slicing exists only to approximate catch-up on a *live*
source, and is the weaker mode's concern.

This is a restructure of the reindex orchestrator, not a localized fix, and is scoped accordingly.

## Not approved by this entry

A specific new public flag, and any unmeasured full-rescan fallback. Those need their own entries.

Also **not** authorized: any production incident repair. No alias changes, deletions, or snapshot operations
against production. When a repair is authorized separately it must require a **successfully completed, verified
backup first**, and must distinguish genuinely lost documents from intentionally deleted or merged ones. Note two
corrections to the earlier repair sketch in the RCA: `op_type: create` is an overwrite safeguard, **not** an
anti-resurrection safeguard, and `T_flip` is triage evidence, **not** sufficient authorization to restore or
overwrite. Candidates whose provenance is unresolved stay excluded pending review.

## Deferred

- The full strict protocol (`Prepare → Copy → Quiesce → Reconcile → Verify → Switch → CacheHandoff → Complete`),
  which is what would actually make a migration verified. Separate approved change, per the 4i recommendation,
  and it must account for many daily partitions and 5–500 GB indexes. Any write pause it introduces must not be
  described as "brief" without measurements.
- The explicitly weaker live mode with its own distinguishable "copied but not verified" result, plus source
  retention in that path.
- Live-write races for models that *can* catch up (alias still moves before the catch-up pass).
