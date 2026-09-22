#!/usr/bin/env bash
set -euo pipefail
files=()
while IFS= read -r path; do files+=("$path"); done < <(git diff --name-only -- '*.cs')
while IFS= read -r path; do files+=("$path"); done < <(git ls-files --others --exclude-standard -- '*.cs')
test "${#files[@]}" -gt 0
# Whitespace-only validation uses the repository's EditorConfig without resolving optional sibling projects.
dotnet format whitespace . --folder --verify-no-changes --include "${files[@]}" --verbosity diagnostic 2>&1 | tee .audit/results/format.log
! grep -Ei 'Msbuild failed|Project file not found|workspace.*failed' .audit/results/format.log
git diff --check
git add -N src tests docs .agents
git diff --binary > .audit/results/candidate.patch
