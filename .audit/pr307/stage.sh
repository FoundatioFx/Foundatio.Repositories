#!/usr/bin/env bash
set -euo pipefail
test "$GITHUB_REF_NAME" = 'audit/pr-307-final-validation-20260922'
root=$(pwd)
base=13f069f60af1b406ee4dafa4874c1793b5caafba
git add src tests docs .agents
git diff --cached --binary > .audit/results/candidate.patch
git worktree add ../pr307-candidate "$base"
git -C ../pr307-candidate apply "$root/.audit/results/candidate.patch"
cd ../pr307-candidate
git config user.name 'github-actions[bot]'
git config user.email '41898282+github-actions[bot]@users.noreply.github.com'
git add src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexTaskResponseReader.cs src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexTaskRunner.cs src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexer.cs tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.TaskResponse.cs
git commit -m 'fix(reindex): validate wire counters and reject timed-out task results'
git add src/Foundatio.Repositories.Elasticsearch/Configuration/Index.cs tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.ErrorLineage.cs
git commit -m 'fix(compat): authenticate naturally prefixed error-index lineage'
git add src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexTaskCancellation.cs src/Foundatio.Repositories.Elasticsearch/Configuration/ElasticIndexCompatibilityUpgrader.cs tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.CancellationEvidence.cs tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.Task.cs tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration/IndexCompatibilityTests.CutoverSafety.cs
git commit -m 'fix(compat): require positive task termination and preserve cancellation'
git add docs/guide/index-management.md .agents/skills/foundatio-repositories/references/index-lifecycle.md src/Foundatio.Repositories.Elasticsearch/Configuration/IElasticConfigurationCompatibility.cs
git commit -m 'docs(compat): document strict task and cancellation evidence'
test -z "$(git status --porcelain)"
git diff --exit-code "$base" HEAD -- .github
test -z "$(git ls-tree -r --name-only HEAD -- .audit)"
git push origin HEAD:refs/heads/audit/pr-307-candidate-20260922
git log --oneline -4 | tee "$root/.audit/results/candidate-commits.txt"
git rev-parse HEAD | tee "$root/.audit/results/candidate-sha.txt"
git bundle create "$root/.audit/results/candidate.bundle" "$base..HEAD"
