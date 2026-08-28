# Contributing

Thank you for contributing to `kuasar-sandbox/orchestrator`.

## Current access model

The organization currently uses a private-fork workflow:

- Contributors can open and triage issues, review changes, and submit pull requests.
- Contributors develop on branches in their own private forks; they do not need write access to the upstream repository.
- Maintainers have upstream write access and are responsible for review, trusted CI execution, and squash merging.
- Direct pushes to upstream `main` are prohibited by project policy.

This model keeps upstream write access limited while the current GitHub plan does not provide enforceable protected-branch rules for private organization repositories.

## Prepare a private fork

Fork the repository to your personal GitHub account, then configure the remotes:

```bash
git clone git@github.com:<your-login>/orchestrator.git
cd orchestrator
git remote add upstream git@github.com:kuasar-sandbox/orchestrator.git
```

Keep your fork's `main` synchronized with upstream:

```bash
git fetch upstream
git switch main
git reset --hard upstream/main
git push --force-with-lease origin main
```

Create a topic branch for every change:

```bash
git switch -c feat/short-description
# edit, test, and commit
git push -u origin feat/short-description
```

Open a pull request from `<your-login>/orchestrator:feat/short-description` to `kuasar-sandbox/orchestrator:main`. Enable **Allow edits from maintainers** when appropriate.

## Pull request requirements

A pull request should:

- have a focused scope and a clear description of the problem and solution;
- link the relevant issue when one exists;
- include or update tests and documentation when behavior changes;
- contain no credentials, private keys, tokens, customer data, or unrelated generated files;
- describe validation already performed and any operational or compatibility risk.

Draft pull requests are welcome for early feedback, but they are not merge candidates.

## Licensing of contributions

By submitting a contribution, you agree that it is licensed under the license
that applies to the files being changed. New project files without a different
explicit license declaration are contributed under the
[Apache License 2.0](LICENSE).

Only submit work that you have the right to contribute. Preserve applicable
copyright, attribution, NOTICE, and SPDX declarations when modifying
third-party or differently licensed material.

## CI and merge policy

The trusted wrapper on the repository's default branch handles
`pull_request_target` actions `opened`, `synchronize`, `reopened`,
`ready_for_review`, and `converted_to_draft`. A workflow file from the candidate
branch never participates in admission. The wrapper calls the central
`kuasar-sandbox/kuasar-sandbox/.github/workflows/bms-entry.yml@main` entry, which
owns admission, source BMS execution, and final status publication.

Admission re-queries the current pull request and compares its state, base ref,
base/head repositories and SHAs, author, and draft state with the triggering
event. A non-draft same-repository pull request is eligible automatically. A
fork pull request is eligible only when its author is currently an active
`kuasar-sandbox` organization member. Draft pull requests do not acquire the
self-hosted BMS runner: their `kuasar/bms-exact-head` status remains `pending`.
Marking a draft Ready emits `ready_for_review`, causing a fresh admission and
BMS run.

For an eligible pull request, admission obtains the current GitHub integration
commit from `.merge_commit_sha`. It requires exactly two parents in order: the
current base SHA, then the reviewed head SHA. Admission writes
`kuasar/bms-exact-head=pending` before automatically entering the central BMS.
An admission rejection writes `failure` when there is a usable integration SHA
and fails closed; a missing or malformed integration SHA cannot receive a
commit status.

An optional `kuasar-bms-companions` block in the pull request body may select
current integration commits from other component pull requests. Admission
validates and records each companion's base, head, two-parent integration, and
current `main`; finalization revalidates the same source set. Pull requests
without this block use the other component repositories' current `main`
revisions resolved during source assembly. The platform source is pinned to the
central workflow revision resolved for that run.

After BMS finishes, the trusted finalizer re-queries the pull request. It writes
`kuasar/bms-exact-head=success` only if BMS succeeded and the pull request is
still open, non-draft, based on `main`, and still has the admitted base, head,
integration commit, parent order, and companion source set. A successful
`BMS E2E / e2e` job by itself is not merge evidence.

Use the current integration commit's combined status as the merge gate. The
status `target_url` is the BMS run URL. The Actions run's top-level `head_sha`
is normally the pull request head, not the integration SHA. The resolved central
workflow revisions can be audited in the run API's `referenced_workflows`; they
are not the component base SHA and do not need a separate workflow-SHA evidence
field.

The following read-only check records and verifies the current evidence:

```bash
set -euo pipefail

repo=kuasar-sandbox/orchestrator
pr=123

pr_json=$(gh api "repos/$repo/pulls/$pr")
base_sha=$(jq -er '.base.sha' <<<"$pr_json")
head_sha=$(jq -er '.head.sha' <<<"$pr_json")
integration_sha=$(jq -er '.merge_commit_sha' <<<"$pr_json")

integration_json=$(gh api "repos/$repo/git/commits/$integration_sha")
jq -e --arg candidate "$integration_sha" --arg base "$base_sha" \
  --arg head "$head_sha" '
    .sha == $candidate
    and (.parents | length) == 2
    and .parents[0].sha == $base
    and .parents[1].sha == $head
  ' <<<"$integration_json" >/dev/null

combined_status=$(gh api "repos/$repo/commits/$integration_sha/status")
exact_status=$(jq -cer '
  [.statuses[] | select(.context == "kuasar/bms-exact-head")][0]
  ' <<<"$combined_status")
jq -e --arg repo "$repo" '
  .state == "success"
  and (.target_url | startswith("https://github.com/" + $repo + "/actions/runs/"))
  ' <<<"$exact_status" >/dev/null
bms_run_url=$(jq -r '.target_url' <<<"$exact_status")

run_id=${bms_run_url##*/}
run_json=$(gh api "repos/$repo/actions/runs/$run_id")
jq -e '
  .event == "pull_request_target"
  and .status == "completed"
  and .conclusion == "success"
  and any(.referenced_workflows[]?;
    .path == "kuasar-sandbox/kuasar-sandbox/.github/workflows/bms-entry.yml@main"
    and .ref == "refs/heads/main")
  and any(.referenced_workflows[]?;
    (.path | startswith("kuasar-sandbox/kuasar-sandbox/.github/workflows/bms-e2e.yml@"))
    and .ref == "refs/heads/main")
  ' <<<"$run_json" >/dev/null

final_pr_json=$(gh api "repos/$repo/pulls/$pr")
jq -e --arg base "$base_sha" --arg head "$head_sha" \
  --arg integration "$integration_sha" '
    .base.sha == $base
    and .head.sha == $head
    and .merge_commit_sha == $integration
  ' <<<"$final_pr_json" >/dev/null

final_status=$(gh api "repos/$repo/commits/$integration_sha/status")
jq -e --arg run "$bms_run_url" '
  [.statuses[] | select(.context == "kuasar/bms-exact-head")][0]
  | .state == "success" and .target_url == $run
  ' <<<"$final_status" >/dev/null

printf 'base=%s\nhead=%s\nintegration=%s\nBMS=%s\n' \
  "$base_sha" "$head_sha" "$integration_sha" "$bms_run_url"
```

Run this check again immediately before merging. Any base, head, or integration
change invalidates the old evidence. An exact-head success proves that finalize
revalidated the admitted companion source set before publishing that status; a
later companion selection or revision is not encoded in the primary integration
SHA. Adding, removing, or editing the `kuasar-bms-companions` block—or an
update to a selected companion—after success invalidates that evidence. Because
body edits are not a supported wrapper event, convert the pull request to draft
and mark it Ready again, then wait for the new exact-head result.

When the primary base, head, and integration are unchanged and a failure is
confirmed to be transient infrastructure, the current workflow run may also be
rerun. When any primary value changed, do not rerun an old event: produce a new
supported pull request event by updating/rebasing the head, or, when there is no
code change, convert the pull request to draft and mark it Ready again. Do not
restore or temporarily add `workflow_dispatch`.

After review conversations are resolved and current exact-head evidence is
successful, squash merge manually. Auto-merge, merge commits, and rebase merges
are not used.

## Review expectations

Be constructive and specific. Authors should resolve review conversations or explain why a requested change is not applicable. Maintainers may close stale, superseded, unsafe, or out-of-scope pull requests with an explanation.
