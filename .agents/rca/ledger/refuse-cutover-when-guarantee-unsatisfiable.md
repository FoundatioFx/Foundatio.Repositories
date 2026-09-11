# Ledger: refuse automatic cutover when the migration guarantee cannot be satisfied

Status: **approved policy, implementation pending**
Approved: 2026-09-11
Scope gate: clarifies existing plan sections 4c/4d and release blocker B1. Does **not** expand scope.

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

## Tests (written first)

1. **Characterization, must keep passing:** a static, date-free, custom-id copy succeeds. This pins that the
   supported configuration is not broken by the new refusal.
2. **False success, must go red first:** using a deterministic pre-cutover hook (baseline-compatible), insert
   and refresh a known custom-id document *after* the initial copy completes but *before* cutover. Assert the
   desired safe outcome (included before promotion, or promotion refused).
3. **Same-count variant:** as above, but *modify* an existing document instead of inserting a new one, so
   document counts match and only content differs. Counters cannot catch this.
4. **Strict procedure:** verify those changes are either included before promotion or promotion is refused.
5. **Weaker mode:** its reporting is distinguishable from verified, and the old index is retained.
6. **Both entry points:** the failure reaches direct callers *and* the queued handler — never a successful
   acknowledgment, and never an endless retry of a configuration that cannot succeed.

## Files and public contracts affected

- `src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs` — phase enforcement before Switch.
- `src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItem.cs` — mode selection (serialized shape: see
  compatibility below).
- `src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItemHandler.cs` — unsupported-configuration
  failures must not retry forever.

## Compatibility impact (to carry into release notes)

- **Default behavior changes:** a live migration that cannot account for concurrent changes no longer promotes
  automatically. Previously it promoted and reported success.
- **Serialized work items:** `ReindexWorkItem` is queued, so already-enqueued items deserialize without the new
  mode field. The default for a missing value must be the safe mode, and that default must be stated.

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
