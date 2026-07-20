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

## CI and merge policy

Fork pull requests must not execute untrusted code on the privileged self-hosted BMS runner. After review, a maintainer may stage the pull request's exact head commit on a trusted upstream CI branch and run `BMS E2E / e2e`.

The pull request head SHA is authoritative:

1. Record the pull request's current head SHA.
2. Run the required checks for that exact SHA.
3. Confirm `BMS E2E / e2e` succeeded.
4. Re-check that the pull request head has not changed.
5. Squash merge manually.

Any new commit invalidates earlier approvals and CI evidence and requires re-validation. Auto-merge is not used. Merge commits and rebase merges are not used.

## Review expectations

Be constructive and specific. Authors should resolve review conversations or explain why a requested change is not applicable. Maintainers may close stale, superseded, unsafe, or out-of-scope pull requests with an explanation.
