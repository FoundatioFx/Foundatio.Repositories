# Ledger: quiesce the source so the cutover commits last

Status: **implemented (opt-in)** — 2026-09-14
Scope gate: takes up the item deferred by
[refuse-cutover-when-guarantee-unsatisfiable.md](refuse-cutover-when-guarantee-unsatisfiable.md) ("the full
strict protocol") and the residual risk it listed under Deferred. Also tightens defect A on the default path,
which that entry could not do because there was no remedy to point at.

## Why the previous entry's residual risk was understated

That entry described what remained as one live-write race. Verified against the code, it is three distinct
defects, and the `_seq_no` guard covers **none** of them because it early-returns for anything that `CanCatchUp`:

- **A — missed updates.** Catch-up without a `TimestampField` ranges on `_id >= ObjectId(startTime)`, which
  encodes **creation** time. An in-place update to a pre-existing document is never caught up, and that branch
  recorded **no `_seq_no` baseline**, so nothing detected it.
- **B — stale overwrite.** Catch-up uses `Conflicts.Proceed` with internal versioning and no `op_type`, so it
  **unconditionally overwrites**. A document written through the alias post-cutover is clobbered by the older
  source copy. Affects **all models, including `IHaveDates`** — internal versioning raises no conflict, so
  `version_conflicts` stays zero, and an overwrite leaves document counts unchanged, so
  `VerifyDocumentCountsAsync` is satisfied.
- **C — resurrected deletes.** No reindex slicing mode can express a delete, so a post-cutover delete is
  re-copied from the source. Affects **all models**. The resurrection makes counts *match*.

Cause is structural: `SwitchAliasesAsync` commits before `RunCatchUpPassAsync` reconciles.

## What was implemented

**`QuiesceSource` is opt-in and off by default.** It trades silent loss for a write outage whose duration is
proportional to how much data must be copied, and is unmeasured on 5–500 GB indexes. Silently introducing a write
outage on upgrade would be worse than the bug. Exposed as `Index<T>.QuiesceSourceOnReindex`, flowed through
`VersionedIndex`/`DailyIndex` into `ReindexWorkItem.QuiesceSource` (an `init` property alongside
`ReindexBatchSize`).

`ElasticReindexer.ReindexAsync` now branches on it: `ReconcileThenPromoteAsync` when set (and
`OldIndex != NewIndex`), `PromoteThenReconcileAsync` otherwise. The default path's ordering, its advisory count
comparison, and its `PromotedButUnconfirmed` handling are untouched.

Quiesced ordering: **Copy (0–90) → Block → CatchUp to convergence → Reconcile deletes → Settle → Verify (hard
gate) → Switch (99) → Record completion → Unblock → Cleanup.** Progress bands that collided on 92 were re-based.

- **`IndexWriteBlock`** — `IAsyncDisposable` applying `PUT /{index}/_block/write` through the raw transport,
  validating `shards_acknowledged` **and** the per-index `blocked` flag rather than trusting a 200. Release is
  best-effort-but-loud: it logs at Error and surfaces if no other exception is in flight, because a stuck block
  leaves an index read-only. A pre-existing block is detected and left in place — something else owns it.
- **Convergence from `_seq_no`, not copy counters.** The previous entry's sketch was "replay until a pass reports
  zero created and zero updated". Measured on a live cluster: `_reindex` reports a byte-identical rewrite as
  `updated`, so that condition never becomes true and the loop would not terminate. `ConvergeCatchUpAsync`
  instead repeats while the source's max `_seq_no` advances, and `EnsureSourceHasSettledAsync` polls it up to
  `MAX_SETTLE_CHECKS` (5) times, failing if it still advances despite the block — which would mean the block was
  bypassed. The catch-up pass rescans the **whole** source, which is what catches in-place updates that no range
  query can express; affordable because it runs once against a source that cannot change, and it removes the
  `TimestampField`/ObjectId classification from this path entirely.
- **Delete reconciliation** (`ReconcileDeletesAsync`) scrolls the destination and `_mget`s each batch against the
  blocked source, deleting destination documents the source no longer has. Skipped when a reindex `Script` is in
  use, since `ctx.op = 'noop'` makes "absent from the source" ambiguous with "deliberately dropped".
- **Verification is a hard gate here.** `VerifyDocumentCountsAsync`'s "never throws" contract is justified only
  by the alias having already moved. That justification does not hold when the alias is still on the source, so
  this path throws `ReindexIncompleteException` on a shortfall — which costs nothing, because the alias has not
  moved and the block is released on the way out.
- **Time-series quiesces per partition**, not all partitions: an all-partitions block would span the entire
  multi-partition migration. The block is released before the next partition starts. The mixed-version window
  across the base alias is pre-existing and unchanged. Both the delete-reconcile and settle loops report progress
  on every batch/check, which is what keeps the 20-minute reindex lock renewed while a partition is blocked.
- **Redelivery narrowed.** `PromotedButUnconfirmed` exists only because the alias moves early. Under quiesce,
  promotion *implies* reconciliation and verification succeeded, so the only thing that can be missing is the
  record write. `GetRedeliveryDispositionAsync` returns the new `PromotedAfterVerification` in that case, and the
  handler re-derives the record via `ElasticReindexer.RecordVerifiedCompletionAsync` (guarded on `QuiesceSource`)
  and acknowledges. The default path still escalates to `ReindexCompletionUnknownException`. The completion record
  is kept — it is still the only durable evidence, and the default path still needs it.

**Defect A tightened on the default path.** The ObjectId branch of `PlanCatchUpAsync` now records a `_seq_no`
baseline (`CatchUpPlan.StartingMaxSequenceNumber`, flagged `CatchUpIsCreationTimeOnly`).
`EnsureNoUncatchableChangesAsync` reads back documents whose `_seq_no` exceeds the baseline (capped at
`CHANGED_ID_SAMPLE_SIZE`, 1000) and compares each id, ordinal, against the catch-up watermark
`ObjectId(startTime)`. It refuses only for ids sorting **below** the watermark — i.e. documents that already
existed and were updated in place, which no id range can reach.

The plan left open whether create-then-update produces enough false positives to prefer a warning over a
refusal. Measured: it does not. A document created during the copy sorts *above* the watermark regardless of how
many times it is subsequently updated, so it is reachable by the catch-up range and does not trigger the
refusal. `ReindexAsync_WhenObjectIdDocumentsAreOnlyCreatedDuringCopy_StillPromotes` pins that. Refusal chosen.

**Explicitly not claimed:** the check cannot see a **delete** during an ObjectId-based copy, because a deleted
document leaves no sequence number behind to find. Defects B and C are not fixed on the default path at all —
they are structural to the ordering, and quiescing is the only remedy.

## Tests (RED before, GREEN after)

```bash
dotnet test tests/Foundatio.Repositories.Elasticsearch.Tests/Foundatio.Repositories.Elasticsearch.Tests.csproj \
  --filter "FullyQualifiedName~ReindexTests|FullyQualifiedName~IndexWriteBlockTests"
```

- `IndexWriteBlockTests` (12): block applied / writes rejected / reads allowed; dispose restores writes; dispose
  runs on the exception path; dispose runs on the cancellation path; **dispose does not mask the caller's
  exception** when release fails (RED-proved: a throwing dispose replaces `InvalidOperationException` with the
  cleanup failure, losing why the migration failed); `ReleaseAsync` *does* throw when release fails;
  release-then-dispose issues the request once; double-dispose is safe; a pre-existing block is preserved; a
  nonexistent index throws; null/empty index throws.
- `QuiescedReindex_WhenDocumentIsUpdatedDuringCopy_PromotesTheNewerValue` — closes **B**.
- `QuiescedReindex_WhenDocumentIsDeletedDuringCopy_DoesNotResurrectIt` — closes **C**.
- `QuiescedReindex_WhenPreExistingDocumentIsUpdatedDuringCopy_StillCopiesTheNewValue` — closes **A**.
- `QuiescedReindex_WhenReconcileFails_RestoresWritesToTheSource` — the block is removed when reconcile throws.
- `QuiescedReindex_DoesNotPromoteTheAliasUntilTheCopyIsReconciled` — the alias is still on the source throughout
  reconcile, which is the ordering claim itself.
- `QuiescedReindex_ForTimeSeriesIndex_BlocksOnePartitionAtATimeAndReleasesEach` — exactly one partition blocked at
  any moment, both released after.
- `QueuedQuiescedReindex_WhenPromotedWithoutCompletionRecord_RecordsCompletionAndAcknowledges` — abandoned without
  quiesce, acknowledged with it.
- `ReindexAsync_WhenPreExistingObjectIdDocumentIsUpdatedDuringCopy_RefusesToPromote` and its false-positive
  counterpart `..._WhenObjectIdDocumentsAreOnlyCreatedDuringCopy_StillPromotes`.
- Regression: 77/77 across `ReindexTests` + `IndexWriteBlockTests`; default-path behavior unchanged.

## Verified findings that shaped the implementation

Established empirically, not assumed.

1. **`_reindex` reports byte-identical rewrites as `updated`.** This invalidated the counter-based convergence
   condition inherited from the previous entry. `_seq_no` advance is used instead.
2. **The settings API reports `index.blocks.write` as the string `"true"`, not a boolean.** Response parsing
   accepts both `JsonValueKind.True` and the string.
3. **There is no unblock API.** Release is `PUT _settings` with `{"index.blocks.write": null}`, issued via raw
   transport because the typed settings descriptor serializes a `null` block to an empty object, which is a no-op.
   Release deliberately ignores the caller's token — unblocking is mandatory cleanup that a cancellation must not
   abandon — but is bounded by its own 30s timeout, matching what `TryCancelTaskAsync` already does for the same
   reason. Removal is split in two: `ReleaseAsync` throws (success path, where a read-only index is a failed
   migration) and `DisposeAsync` only logs (backstop, where throwing from a `finally` would replace the exception
   that caused the unwind).
4. **`_id` cannot be sorted on** (`Fielddata access on the _id field is disallowed`), so delete reconciliation
   pages with the scroll API. `_source: false` must go on the query string; ES rejects it in an `_mget` body.
5. **`ObjectId.GenerateNewId(DateTime)` sorts correctly against the watermark**, and ordinal string comparison is
   sufficient — confirmed with a probe placing a seeded id below the watermark and a during-copy id above it.

## Compatibility impact (carried into docs and the PR description)

- **`QuiesceSourceOnReindex` is off by default**, so nothing changes for anyone who does not set it. When set,
  **writes to the source are rejected with `403 cluster_block_exception` for the duration of the copy**; reads are
  unaffected. The outage is proportional to index size and is not brief on a large index. For time-series it
  covers one dated partition at a time.
- **Default behavior change (defect A):** a migration with ObjectId ids and no timestamp field now throws
  `ReindexIncompleteException` when a **pre-existing** document is updated in place during the copy. This can fail
  migrations that previously appeared to succeed — they were losing data when they did. Append-only workloads are
  unaffected: creations during the copy are catchable and still promote.
- **Serialized work items:** `ReindexWorkItem.QuiesceSource` is a new `bool` defaulting to `false`, so
  already-enqueued items deserialize to the existing ordering and behavior.
- **Redelivery:** a redelivered work item for a quiesced migration that was promoted without a completion record
  is now acknowledged (record re-derived) instead of raising `ReindexCompletionUnknownException`. The default path
  still raises it.
- **Extra requests:** the ObjectId guard adds one shard-level stats read at plan time plus one stats read and one
  bounded search before cutover. The quiesced path adds a block, one settings write, the convergence/settle
  `_seq_no` reads, and the delete-reconcile scroll — all only when opted in.

## Deferred

- **Measuring the outage.** Duration is not characterized on 5–500 GB indexes. Docs state it is proportional to
  index size and refuse to call it brief; they do not offer a number.
- **A weaker explicit "copied but not verified" live mode** with its own distinguishable result, still deferred
  from the previous entry.
- **Defects B and C on the default path.** Not fixable without moving the commit point, which is what quiesce
  does. The default path remains as documented under "Remaining limitations".
- **Deletes during an ObjectId-based copy** on the default path, which leave no `_seq_no` to detect.
