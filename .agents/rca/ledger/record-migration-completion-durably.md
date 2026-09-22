# Ledger: record migration completion durably so a redelivery cannot ack an unconfirmed migration

Status: **implemented (bounded scope expansion)** — approved 2026-09-11, implemented same day. See the
2026-09-22 review below for changes and unresolved recovery limitations; the original validation is historical.
Scope gate: bounded addition to the reindex safety work. Explicitly **not** authorization for the full
strict-migration protocol.

## Problem

A queued work item whose destination was already promoted but whose migration never completed was acknowledged
as successful. `ReindexWorkItemHandler.IsAlreadyReindexedAsync` read the index version, found it at or past the
target, reported progress 100 and completed the item.

The alias switch happens **before** the catch-up pass, so a first attempt that fails after the cutover leaves
the version already advanced. That state is indistinguishable from a migration another process finished — both
leave the same observable cluster state. The queue retry that the docs present as the recovery mechanism
therefore recorded a known-short migration as complete and could never recopy the missing documents.

Found by a Bugbot review of the branch. It is the queued twin of the `ElasticConfiguration.ReindexAsync` retry
defect fixed in `2d77dd00`; both came from the same root assumption that alias state implies completion.

## What was implemented

- `ReindexCompletion` record + `foundatio-reindex-completions` index (`dynamic: strict`, single shard, keyword
  fields, no expiry). One small document per physical migration, fetched only by id.
- `RecordCompletionAsync` writes it after every failure-capable step succeeded, **before** the queue ack and
  **before** source deletion. Cleanup is downstream, so a delete failure cannot demote a finished migration.
  If the record cannot be persisted, `ReindexAsync` throws rather than reporting an unvouchable success.
- `HasCompletionEvidenceAsync` / `TryReadCompletionAsync` validate a record against this migration's identity,
  the destination's **current** UUID, and the transformation fingerprint. Any mismatch is treated as no
  evidence.
- `ReindexWorkItemHandler` originally made a three-way decision (`RedeliveryDisposition`): `SafeToStart`,
  `AlreadyCompleted`, `PromotedButUnconfirmed`. The old boolean skip became `IsAlreadyPromotedAsync`, which
  answers only "has the cutover happened?". A later quiesce change added `PromotedAfterVerification`; see the
  review below for why the current request's quiesce flag is insufficient provenance.
- `ReindexCompletionUnknownException` for the unconfirmed case — neither success nor permission to copy again.
  Both indexes are preserved; no replay, rollback, or deletion. Throwing routes it into the queue's own
  abandon/retry/dead-letter handling with the work identity intact.

### Identity design

Keyed by `alias|oldIndex|newIndex` — the logical migration. Deliberately **not** an attempt, delivery, or job
id: those change on redelivery, which would make every retry look like a migration that had never run. The
lookup does not involve the source index, so it still works after legitimate source cleanup.

Bound additionally to:

- **Destination UUID** — index names are reusable, so a record left by an index since deleted and recreated
  under the same name must not match.
- **Transformation fingerprint** (SHA-256 of `Script`, or `"none"`) — an unscripted copy must not vouch for a
  scripted one over the same indexes.

Writes are idempotent (same id, same content). An ambiguous write response is resolved by reading the exact
record back, never by assuming it saved or blindly rerunning.

## Verified findings that affect implementation

- **`_meta` on the destination index was evaluated and rejected.** It looked ideal (no new shards, correct
  lifecycle, survives source cleanup) and was confirmed on a live cluster to survive unrelated mapping updates.
  But a `_meta` write **replaces** rather than merges: writing one key wiped a previously written key. That
  would clobber consumer metadata, so a dedicated index is used instead.
- **The `MigrationState` pattern was reused; its index was not.** `MigrationManager.GetMigrationStatus`
  computes `currentVersion` from *all* documents in the `migration` index, so adding reindex records there would
  corrupt unrelated migration bookkeeping. The suitable parts — durable ES document, keyed reads, immediate
  consistency, no expiry — were reused via the same shape as the existing `{index}-error` index.
- **Queue disposition was verified against `WorkItemJob` source**, not assumed: a handler throw maps to
  `AbandonAsync`, which redelivers while `Attempts < Retries + 1` and then dead-letters. `WorkItemContext.Result`
  independently affects disposition. Tests assert `Completed`/`Abandoned`/`Deadletter` counters rather than
  merely that the handler threw.
- **Completion index name is not derived from the migrated index name**, so it is never matched by the
  `{name}-v*` patterns used to enumerate and delete index versions.

## Tests (end-to-end first, RED before the fix)

Reproducer drives the **real queue and `WorkItemJob`**, because a handler-level assertion cannot distinguish a
successful ack from a silent one.

- `QueuedReindex_WhenDestinationPromotedButMigrationNeverCompleted_IsNotAcknowledgedAsSuccess` — RED at
  `Completed = 1` (commit `8f31eff5`). Uses an **equal-count, wrong-content** destination so no count or
  progress check can rescue the decision. Asserts both indexes survive.
- `QueuedReindex_WhenCompletedThenRedeliveredToFreshHandler_IsAcknowledgedWithoutRecopying` — lost ack, new
  handler instance, source deleted so a silent re-copy would have to fail loudly.
- `QueuedReindex_WhenDirectMigrationCompletedIt_IsAcknowledgedAsAlreadyDone` — cross-path recognition.
- `QueuedReindex_WhenCompletionRecordNamesAnotherIndexGeneration_IsNotAcknowledgedAsSuccess` — stale UUID.
- `QueuedReindex_WhenCompletionRecordWasWrittenForAnotherTransformation_IsNotAcknowledgedAsSuccess` — mismatched
  script.
- `QueuedReindex_WhenSourceCleanupFailsAfterCompletion_StillAcknowledgesTheMigration` — cleanup failure must not
  restart copying.
- `ReindexAsync_WhenCompletionCannotBeRecorded_DoesNotReportSuccess` — unwritable record fails closed.
- `ReindexAsync_WhenCompletionIsWrittenTwice_RemainsSingleValidRecord` — idempotent write, one record, still
  valid.

The three refusal tests assert the exact exception type, and a **mutation probe** (returning `SafeToStart`
instead of `PromotedButUnconfirmed`) failed all three — confirming they are load-bearing rather than vacuous.

Historical regression result from the original implementation: 188/189 across reindex, migration, and index
suites. That run attributed one failure to a Docker port-forwarder stall that passed in isolation. This is
not a validation result for subsequent revisions.

## 2026-09-22 review: exact destination and recovery provenance

`f9e02ec` replaces the configuration-version shortcut with an alias lookup for the work item's exact physical
`NewIndex`. An old v1-to-v2 item must not become replayable merely because its worker now configures v3, and a
promoted daily/monthly partition must not become replayable because another partition is still on v1. Both
handler constructors now use the same check. Unavailable or incomplete alias metadata fails closed.

Completion evidence is checked before alias state so a recorded migration can still be recognized after its
alias moves on. Fifteen offline cases in `ReindexWorkItemHandlerTests` cover exact and dated destinations,
case-sensitive alias matching, missing indexes, failed/incomplete responses, and pre-cancelled requests.
`40a3249` supplies the shared error-formatting import required by both target frameworks. Test results must be
read from the corresponding CI revision; the presence of these tests is not a claim that they have passed.

**Unresolved:** `PromotedAfterVerification` still infers the earlier attempt's protocol from the current work
item's `QuiesceSource` flag. A default-order attempt may have promoted and failed before a different item is
submitted with that flag enabled. Neither the new flag nor the alias proves that the earlier copy was
verified. Recovery must require durable prior-attempt evidence or remain `PromotedButUnconfirmed`. The exact
index lookup does not resolve this separate review finding.

The script-only fingerprint also does not bind `StartUtc`, `TimestampField`, or `QuiesceSource`, and the
concatenated completion id is not length-bounded. Those identity concerns remain separate from destination
lookup and require regression coverage before claiming completion records attest the full migration contract.

## Compatibility impact (to carry into release notes)

- **New index**: `foundatio-reindex-completions`, one single-shard index, one small document per physical
  migration. Not matched by `{name}-v*` enumeration or deletion patterns.
- **Migrations completed before this release have no record.** For default-order work items, a promoted target
  without a record produces `ReindexCompletionUnknownException` instead of acknowledgment. The current
  quiesced recovery exception is subject to the unresolved provenance finding above. Ordinary startup is
  unaffected: nothing verifies existing indexes and no scan is introduced.
- **New exception type** `ReindexCompletionUnknownException` on the queued path.
- No new `ReindexWorkItem` fields in the original completion-record change; work items remain wire-compatible.
- No per-document records, no new scans, no new storage framework, no generic workflow engine.

## Not claimed

A completion record attests that the copy met the contract this branch implements — the copy task accounted for
every document it matched, the catch-up pass completed or was proven unnecessary, and the aliases moved. It is
**not** proof of strict or lossless consistency. In particular, the unresolved recovery-provenance and
fingerprint limitations above prevent treating every currently accepted record as proof of that full contract.

## Deferred (unchanged by the original entry)

- Write blocking / the full Quiesce protocol. Later changes added opt-in quiescence; its recovery limitations
  are recorded above.
- Phase reordering so the alias moves after the catch-up pass.
- Automatic repair, task-resume orchestration, and broader cache changes.
- Automatic verification of existing large indexes on startup. Any such recovery must stay an explicit,
  operator-initiated request.
