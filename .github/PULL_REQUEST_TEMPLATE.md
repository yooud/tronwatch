## Summary

Describe the problem and the chosen solution.

## Operational impact

Describe changes to configuration, storage, event schemas, resource use, recovery, or deployment. Write `None` when there is no impact.

## Validation

List automated tests and manual checks performed.

## Checklist

- [ ] The change is focused and has tests where behavior changed.
- [ ] `make verify` passes.
- [ ] `make race` passes for concurrency-sensitive changes.
- [ ] User-facing behavior and configuration are documented.
- [ ] Compatibility or migration requirements are described.
- [ ] No credentials, local configuration, runtime data, or private endpoints are included.
