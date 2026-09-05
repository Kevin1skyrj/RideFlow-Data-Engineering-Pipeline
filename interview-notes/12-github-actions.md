# 12 — GitHub Actions and CI/CD

## Concepts

A workflow is a YAML automation triggered by events. It contains jobs; jobs run
on runners and contain ordered steps. Jobs are isolated unless artifacts or
explicit dependencies connect them. Matrix strategy repeats a job across
configurations such as Python versions.

CI validates integration continuously. CD publishes or deploys an artifact
after validation.

## RideFlow workflows

The CI workflow tests quality on Python 3.12/3.13, a clean warehouse path, and
the real Kafka Compose path. The Pages workflow deploys the static dashboard.
Artifacts preserve exported marts and dbt run results for inspection.

Concurrency cancels superseded runs. Minimal `contents: read` permission follows
least privilege for CI.

## Questions and answers

### “Why matrix testing?”

> The package declares Python 3.12+, so testing only my local 3.13 would leave
> that compatibility claim unverified. The matrix checks both supported minor
> versions independently.

### “Why a clean-checkout job?”

> It detects undeclared files, directories, environment variables, and cached
> state. That job exposed my JSONL-versus-Parquet mismatch, which local generated
> data had hidden.

### “Why upload artifacts on failure?”

> dbt results and partial exports provide evidence after the ephemeral runner is
> gone, reducing diagnosis time. Upload is `always()` and missing files warn
> rather than hiding the original error.

### “Why separate CI and Pages deployment?”

> They have different permissions, triggers, and failure meaning. Validation
> should not require deployment rights, and a Pages configuration failure should
> not be confused with broken data logic.
