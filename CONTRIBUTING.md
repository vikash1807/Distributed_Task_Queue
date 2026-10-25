# Contributing

Contributions are welcome, including bug reports, documentation improvements,
tests, and focused features.

## Before you start

1. Read the [README](README.md) for the product and quick start.
2. Follow the [development guide](docs/develoment.md) to set up your environment.
3. Read the [architecture guide](docs/architecture.md) before changing queue,
   scheduling, worker, or persistence behavior.
4. Follow the repository's [agent instructions](AGENTS.md) when using a coding agent.

## Report a bug or propose a change

For bugs, include the expected behavior, actual behavior, reproduction steps,
relevant configuration, and useful logs. Remove passwords and sensitive payloads.

For features, explain the problem, intended behavior, and how success can be
verified. Discuss substantial API, storage, or deployment changes in an issue
before starting implementation.

## Work on a focused branch

Create your branch from an up-to-date `main`:

```bash
git switch main
git pull --ff-only
git switch -c feat/short-description
```

Use a descriptive prefix such as `feat/`, `fix/`, `docs/`, or `test/`. Keep each
pull request focused on one change or a closely related set of changes. Preserve
unrelated work already present in your checkout.

## Make the change

- Follow the component boundaries and invariants in the architecture guide.
- Add short docstrings to new or modified classes, functions, and methods.
  Explain important decisions and non-obvious logic with inline comments.
- Add tests for meaningful behavior and failure cases. Documentation-only changes
  need relevant documentation checks rather than new application tests.
- Update the appropriate document under `docs/` when behavior changes. Update
  `.env.example` when settings change, and keep README focused on product use.
- Keep formatting and unrelated cleanup out of the change unless needed for it.

See the development guide for coding practices, dependency management, and safe
Redis integration-test setup.

## Validate your work

Run these checks for code changes, replacing the example paths with your changed
files:

```bash
uv run pytest -q
uv run ruff check app/path/to/file.py tests/test_feature.py
uv run ruff format --check app/path/to/file.py tests/test_feature.py
git diff --check
```

Redis integration suites skip unless dedicated test URLs are set. For Redis,
lease, scheduling, or worker changes, run the relevant integration suites using
the [disposable Redis setup](docs/develoment.md#redis-integration-tests).
**Their fixtures call `FLUSHDB`; never point them at shared or application data.**

For documentation changes, check relative links, examples, configuration names,
and consistency with the implementation. Always run `git diff --check`.

## Write useful commits

Use concise messages describing the result:

```text
feat(cron): add enabled-state updates
fix(worker): preserve heartbeat during drain
docs: explain local test setup
```

Prefer one logical change per commit. Reference the relevant issue; use a closing
footer such as `Closes #123` only when the change resolves it.

## Open a pull request

Target `main` and include:

- The problem and resulting behavior.
- The implementation decisions a reviewer needs to assess.
- Commands run, results, and any skipped checks.
- Relevant issue references and closing keywords.
- Compatibility, configuration, or operational changes, if applicable.

Before requesting review, confirm that the diff contains only intended changes,
checks have passed, documentation is current, and no credentials or local
artifacts are included. Respond to feedback with focused follow-up commits.
