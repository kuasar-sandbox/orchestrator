# Contributing

Thank you for contributing to `kuasar-sandbox/orchestrator`.

Documentation changes follow the shared [documentation contribution policy](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/CONTRIBUTING.md#documentation-contributions).

## Contribution workflow

Use a public Fork and a topic branch. Contributors can open issues, review changes
and propose PRs without upstream write access. Maintainers review changes, arrange
trusted validation and squash merge; direct pushes to upstream `main` are not the
contribution path.

## Prepare a Fork

Fork the repository to your GitHub account, then configure HTTPS remotes:

```bash
git clone https://github.com/<your-login>/orchestrator.git
cd orchestrator
git remote add upstream https://github.com/kuasar-sandbox/orchestrator.git
git fetch upstream
git switch -c feat/short-description upstream/main
# edit, test, and commit
git push -u origin feat/short-description
```

For a maintenance fix, start from the supported `upstream/release/vMAJOR.MINOR.x`
branch instead and target that branch in the PR. Enable **Allow edits from
maintainers** when appropriate.

To advance your Fork's `main` without overwriting local commits or uncommitted work:

```bash
git fetch upstream
git switch main
git merge --ff-only upstream/main
git push origin main
```

Commit or safely preserve uncommitted work before switching. If `--ff-only`
refuses because histories diverged, inspect the local commits and resolve the
history deliberately; do not reset or force-push the Fork's main branch as a
routine synchronization step. Create each new topic branch from the freshly
fetched upstream target, not from an unrelated feature branch.

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

The repository's trusted default-branch wrapper handles pull-request events;
candidate workflow files never decide admission or obtain control-plane secrets.
The [central CI contract](https://github.com/kuasar-sandbox/kuasar-sandbox/blob/main/docs/ci.md)
owns the current workflow entry points, exact status names, companion declaration
syntax, source selection and executable evidence checks. Do not copy those
identifiers into another independently maintained workflow guide.

All contributors may submit public Fork PRs. Access to a privileged integration
runner is a separate trust decision, not a condition for opening an issue or PR.
Automatic candidate execution accepts non-draft same-repository PRs and Fork PRs
whose author is an active organization member; it does not run arbitrary external
Fork code with organization credentials. For other contributors, a maintainer
reviews the change and may prepare a trusted upstream candidate PR, preserving
commit authorship, linking the original discussion and recording the source and
reviewed diff. The trusted candidate receives the normal tests and review. Do not
change membership, weaken admission or execute unreviewed Fork code merely to get
a check result. Drafts defer integration execution and are not merge candidates.

Before normal squash merge, verify all of the following against live GitHub state:

1. The PR is open and ready, targets `main` or a supported
   `release/vMAJOR.MINOR.x`, and all blocking review conversations are resolved.
2. The admitted integration commit has exactly the current target base and
   reviewed head as its ordered parents. The successful exact-integration status
   belongs to that commit, not merely to the PR head or an old run.
3. The linked trusted run completed the required tests; a skipped, cancelled or
   failed execution is not successful evidence. Its resolved control and execution
   workflows, source set and result must agree with the central CI contract.
4. Each declared companion was independently admitted and tested. Its repository,
   PR, base, head, target ref and integration commit still match the recorded set.
   A primary PR's success never substitutes for a companion's own required check.
5. Base/head changes, companion changes or edits to the companion declaration
   invalidate prior evidence. Produce a fresh supported PR event and verify the
   new result. Body edits alone do not trigger validation; mark Draft and then
   Ready again when a new event is needed without a new commit.

A rerun of an existing event is appropriate only for a diagnosed transient failure
when its admitted base, head, integration and companion inputs remain unchanged.
Do not add a manual-dispatch entry, manufacture a successful status or bypass
branch protection. Recheck the current source set immediately before merging.
After one companion merges, remove its obsolete declaration from the remaining
PRs and rerun against the selected target-line sources.

Candidate execution must not retain the source-fetch App token or inherit release
write credentials. Keep execution and control identities, caches and workspaces
within their documented trust boundaries. Auto-merge, merge commits and rebase
merges are not used; maintainers perform the normal reviewed squash merge.

## Review expectations

Be constructive and specific. Authors should resolve review conversations or explain why a requested change is not applicable. Maintainers may close stale, superseded, unsafe, or out-of-scope pull requests with an explanation.
