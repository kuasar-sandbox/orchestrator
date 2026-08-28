## Summary

<!-- What problem does this change solve, and how? -->

## Related issue

<!-- Use "Closes #123" or link the relevant issue. -->

## Validation

- [ ] I ran the relevant local/unit/race tests.
- [ ] I added or updated tests for changed behavior, or explained why none are needed.
- [ ] I updated documentation or configuration examples when applicable.
- [ ] This change contains no credentials, tokens, private keys, customer data, or unrelated generated files.

Commands and results:

```text

```

## Compatibility, risk, and rollback

<!-- Describe compatibility impact, operational risk, migrations, and rollback steps. -->

## Maintainer merge gate

- [ ] The change has completed code review.
- [ ] Review conversations are resolved.
- [ ] The current base SHA and reviewed pull request head SHA have been recorded.
- [ ] The integration commit has exactly two parents: the recorded base, then the recorded head.
- [ ] `kuasar/bms-exact-head` is `success` on that integration SHA.
- [ ] The exact-head status points to a completed, successful `pull_request_target` BMS run.
- [ ] A final query confirmed that the base, head, and integration SHA are unchanged and the exact-head status still succeeds for the recorded run.
- [ ] The pull request will be squash merged manually; auto-merge is not used.

Base SHA:

```text

```

Reviewed head SHA:

```text

```

Integration SHA:

```text

```

BMS exact-head status / run URL:

```text

```

The central workflow revisions, when needed for audit, are available from the
run metadata's `referenced_workflows`; they are not a separate merge-gate field.
