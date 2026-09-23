# Reindex consistency: protocol and remaining release gates

Status: **the generation/attempt journal and automatic takeover protocol below are not implemented**. This document is an explicit acceptance contract, not a claim that the current code provides crash-safe online migration. Related work: Repositories #307/#327 and Foundatio #573.

## Invariants

A lease owner must be able to detect failed renewal. Losing ownership removes permission to initiate cutover, delete artifacts, unblock a source or fabricate completion. Independent heartbeats improve lease liveness but cannot revoke a request already sent to Elasticsearch.

Task submission and task completion are separate facts. A lost launch response must not be blindly retried. `X-Opaque-Id` is correlation, not deduplication. HTTP404 and complete-looking task listings are observations, not proof that every delayed request or relocated writer is gone. Terminal task evidence does not by itself prove the copy is correct or sufficiently replicated.

An exact copy is verified against physical generations, not mutable names. Routed document identity, all expected primary shards, transformation/configuration identity, intended aliases and destination redundancy must be accounted for. Counts cannot establish equality of identities or contents.

## Required durable attempt record

Persist, before launch, a unique attempt ID; logical lock resource; cluster identity; source and destination UUIDs; transformation and relevant mapping/settings fingerprints; intended alias definitions; and ownership generation. Persist task IDs and relocation lineage when learned. A launch with no confirmed task ID remains unknown, not not-started.

State transitions must use optimistic concurrency on the record and explicit allowed prior states: planned, launch intent, copying, verified, cutover intent, committed, completed, uncertain. Verification records must bind to the destination UUID and expected alias topology. Commit intent must be durable before submitting destructive alias/delete operations. Completion must follow verified cutover and required finalization, never be reconstructed from a newly supplied request flag.

Until takeover is proven safe, an unresolved attempt forbids another controller from reusing its target. An updated record or a renewed Redis lease is not an Elasticsearch fencing token. The design must either enforce mutation authority at a single coordinator/gateway that rejects stale generations, or forbid automatic takeover and retain artifacts until outstanding requests are reconciled. Ordinary UUID prechecks alone do not make a later alias update conditional on that UUID.

## Recovery acceptance matrix

| Observation | Permitted action |
| --- | --- |
| Authoritative terminal copy failure, no cutover dispatch, stable authenticated generations | A separately verified reset may discard only the private target; do not generalize this to restarts |
| Task404, unknown launch, node/task failures, missing lineage or duplicate submission | Retain artifacts and authority restrictions; no inferred termination |
| Cutover intent with lost response | Independently inspect exact expected alias topology and physical generations; never assume source is still live |
| Proven committed target bound to durable verified record | Finalize that generation only; do not recopy into the live target |
| Recreated target with the same name | Reject stale records and require manual reconciliation |
| Lost lease while requests are outstanding | Stop new guarded mutations; durable ownership transition alone cannot revoke old requests |

## Required fault tests

Exercise lease expiry/reacquisition and delayed old-owner resumption; failed/late renewal; duplicate asynchronous launch after a lost response; task relocation with original and successor IDs; missing stored task result; delayed alias request after controller restart; source or destination delete/recreate with the same name; missing replicas at destructive cutover; scripted membership/identity changes; same-ID different-routing documents; partial mget/bulk/search/refresh/count results; quiet-shard updates; and changed identities beyond the first page.

Kill and restart the coordinator at every durable transition. Tests must assert what was **not** mutated, not only which exception was raised. Validate the actual combined release tree, not independently green branches. A two-node happy-path suite is not a network-partition or crash-recovery certification.

## Strict offline and low-downtime protocols

Strict offline replacement gates writers, establishes a dedicated source write block, refreshes, copies to an isolated target, verifies task/item/shard/content evidence and replica readiness, then switches routing and retires old writers. Retained old physical indexes remain blocked. For #307 the canonical old physical name becomes an alias, so source deletion is part of cutover and a verified snapshot is required before it.

Low-downtime mutable migration instead needs durable snapshot-boundary plus change capture, ordered idempotent replay including deletions/tombstones, a final writer gate and replay boundary, verification, destination redundancy and controlled retirement. Direct dual writes without atomic durable intent leave a split-write window. Tombstone retention must span every active consumer's replay boundary. A full second scan while blocked remains an O(dataset) outage, not a delta-only migration.

Use immutable historical partitions to reduce the active write window only when late arrivals, corrections and retention jobs are controlled. Slicing, throughput changes and newer relocation-aware task APIs need explicit child-task/lineage accounting. Elasticsearch 9.5's reindex-specific cancellation API can follow shutdown relocations, but its documented 404 is still ambiguous; do not downgrade the older-server safety rule.

## Release boundary

The cache ownership repair must be released and consumed, not merely opened as an upstream PR. Shared migration code must be reconciled before combining #307/#327/#308. Require exact-head full suites, fault tests, packaged consumers, persistent-major/snapshot rehearsal and the deployed cluster's own upgrade checks. Current partial implementations and this design document are not production approval.
