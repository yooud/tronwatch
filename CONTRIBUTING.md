# Contributing to tronwatch

Contributions are welcome. Keep changes focused, explain their operational impact, and preserve the chain and delivery guarantees documented in `docs/architecture.md`.

## Development setup

Requirements:

- Go 1.25.13 or newer;
- Git;
- Docker only when testing the container deployment.

Clone the repository and run the quality gates:

```bash
make verify
make race
```

`make verify` checks formatting, runs the unit tests and `go vet`, and builds the binary. All checks must pass before a pull request is opened.

## Making a change

1. Create a short-lived branch from `main`.
2. Add tests for observable behavior changes and bug fixes.
3. Update README or files under `docs/` when configuration, output, or operational behavior changes.
4. Keep generated protocol code consistent with `internal/protocol/wire.proto`.
5. Run `make verify` and `make race`.
6. Open a pull request describing the problem, the chosen approach, and the validation performed.

Avoid drive-by refactors in feature and bug-fix pull requests. Changes to persistence, fork handling, finality, or delivery semantics should describe compatibility and recovery behavior explicitly.

## Commit messages

Use an imperative subject and a conventional prefix:

```text
feat: add a watch source
fix(store): retain events until every publisher acknowledges
docs: clarify mainnet configuration
test(p2p): cover duplicate block delivery
```

Keep commits independently understandable. Do not commit runtime databases, local configuration, credentials, logs, profiles, or benchmark output.

## Pull requests

Pull requests should be small enough to review as one coherent change. The description must include:

- the user-visible or operational effect;
- any configuration or storage compatibility impact;
- tests and manual checks performed;
- follow-up work that is intentionally out of scope.

Report suspected vulnerabilities through the process in `SECURITY.md`, not through a public issue.

## Cutting a release

Releases are built by GitHub Actions from annotated semantic-version tags.

1. Update `internal/version.Value` and move the relevant changelog entries out of `Unreleased`.
2. Run `make verify`, `make race`, and `goreleaser check`.
3. Test the packaging locally with `goreleaser release --snapshot --clean`.
4. Commit the version and changelog as `chore: release vX.Y.Z`.
5. Create an annotated tag with `git tag -a vX.Y.Z -m "tronwatch vX.Y.Z"`.
6. Push the branch and tag. The tag starts the release workflow and creates the GitHub Release.

Do not move a published release tag. If a release fails after publication, fix the cause and publish the next patch version.
