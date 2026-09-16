# LLD 08 — Environment-Specific Configuration

## Objective

Let the same pipeline code run correctly in Dev, QA, and Prod by changing
only configuration — never code — while keeping business/pipeline metadata
(what tables exist, what their DQ rules are) identical across environments
so Dev testing is representative of Prod behavior (Section 25).

## Assumptions

- Business metadata (`sources.yaml`, `pipelines.yaml`, `dq_rules.yaml`,
  `reconciliation.yaml`, `schemas.yaml`) is environment-agnostic by design —
  a table's DQ rules don't change between Dev and Prod.
- Only infrastructure-shaped values (bucket names, role ARNs, cluster
  sizing, feature flags) differ per environment.
- Secrets are never present in any YAML file, in any environment
  (Section 25, ADR-009).

## Input / Output

**Input:** `ENVIRONMENT` env var (or explicit constructor arg) + the
`config/` directory tree.

**Output:** A merged configuration dict combining all `config/base/*.yaml`
files with the one matching `config/{environment}/environment.yaml`.

## Components

`src/common/config_loader.py::ConfigLoader`.

## Sequence flow

```
ConfigLoader(environment="dev")
  -> _base(): deep-merge sources.yaml + pipelines.yaml + dq_rules.yaml
              + reconciliation.yaml + schemas.yaml into one dict
  -> environment_config(): load config/dev/environment.yaml (lazy, cached)
  -> get_pipeline(name) / dq_profile(name) / reconciliation_profile(name) / s3_path(zone)
     all read from the merged base dict; environment-specific values (bucket
     name, role ARN) are read separately from environment_config()
```

## Pseudocode

```
def _deep_merge(base, override):
    merged = dict(base)
    for key, value in override.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged
```

Used to combine the five base YAML files (each contributing a distinct
top-level key: `pipelines`, `stages`/`defaults`, `profiles` [from both
dq_rules and reconciliation — see note below], `schemas`) into one
namespace, and separately to apply an environment overlay on top of a
shared default block where one exists.

**Note on `profiles` namespace sharing**: `dq_rules.yaml` and
`reconciliation.yaml` both declare a top-level `profiles:` key. The deep
merge unions them into a single `profiles` dict; this works safely because
the two files use disjoint naming conventions (`*_standard` for DQ,
`*_daily`/`*_hourly`/`*_backfill` for reconciliation) — a documented
convention, not an enforced constraint. A stricter implementation would
validate no name collision at load time.

## Exception handling & retry

`ConfigurationError` (a `NonRetryableError`) is raised for: an unknown
environment name, a missing pipeline/profile/schema lookup, or a missing
YAML file entirely. None of these are retried — they represent a
deployment/config bug, not a transient condition; retrying with the same
bad configuration would fail identically.

## Logging & audit

Configuration loading itself is not logged verbosely (it's a startup-time
concern); pipeline code logs which `environment` and `pipeline_name` it
resolved, which is enough to reconstruct "what configuration was this run
using" from the structured log/audit trail.

## Security

Environment separation is the primary security boundary this LLD
addresses: Dev config points only at Dev buckets/roles/clusters, so even a
bug that reads the wrong `ConfigLoader` instance cannot cross into touching
Prod resources — there is no shared "default" bucket that an
unset-environment code path would fall back to (an unknown environment
raises immediately instead of silently defaulting).

## Performance

Configuration is loaded once and cached per `ConfigLoader` instance
(`_base_cache`, `_env_cache`); a long-running Glue job or repeated pipeline
invocations within one process do not re-parse YAML on every call.

## Edge cases

| Case | Handling |
|---|---|
| `ENVIRONMENT` env var unset | Defaults to `"dev"` — safe default (never silently defaults to prod) |
| `ENVIRONMENT=staging` (not a recognized environment) | Raises `ConfigurationError` immediately, not a fallback to Dev |
| A DQ profile referenced in `sources.yaml` doesn't exist in `dq_rules.yaml` | Raises `ConfigurationError` at profile-lookup time, not silently skipped |
| Two base YAML files declare the same non-dict key | Later-loaded file's value wins (deep merge's non-dict branch) — file load order in `_base()` is therefore significant and fixed |

## Test cases

`tests/unit/test_config_loader.py` — pipeline metadata resolution, unknown
pipeline/environment rejection, environment-specific overlay values differ
correctly between Dev and Prod, S3 path resolution, independent DQ/
reconciliation profile resolution.
