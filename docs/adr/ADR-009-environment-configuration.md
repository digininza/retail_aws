# ADR-009: Environment Configuration Strategy

## Context

The same pipeline logic must run correctly and safely across Dev, QA, and
Prod, with zero risk of Dev code accidentally touching Prod data, and
without duplicating business/pipeline metadata three times (Section 25).

## Decision

Split configuration into environment-agnostic base metadata
(`config/base/*.yaml`: sources, pipelines, DQ rules, reconciliation rules,
schemas) and a thin per-environment overlay (`config/{dev,qa,prod}/environment.yaml`:
bucket names, role ARNs, cluster sizing, feature flags). Secrets are never
stored in either — only referenced by ARN, resolved via Secrets Manager at
runtime (`src.common.utilities.SecretsManagerAdapter`).

## Alternatives considered

- **Fully separate config trees per environment (no shared base)** —
  rejected: guarantees drift over time (a DQ rule fixed in Dev's copy but
  not Prod's), and triples the maintenance burden for information that
  should be identical across environments by definition (a table's schema
  doesn't change because it's Tuesday in QA).
- **Environment variables for everything, no YAML** — rejected: unwieldy
  for the amount of structured, nested configuration this platform has
  (dozens of pipelines, each with several metadata fields); YAML with a
  typed loader (`ConfigLoader`) is far more maintainable and reviewable in
  a PR diff.
- **A single config file with environment as a top-level key
  (`{dev: {...}, qa: {...}, prod: {...}}`)** — rejected: makes it easy to
  accidentally reference the wrong environment's block in code, and puts
  all three environments' values in one file that every environment's
  deploy would need read access to (weaker blast-radius containment than
  fully separate per-environment files).

## Rationale

This split directly implements Section 25's requirement: "never duplicate
the entire pipeline configuration unnecessarily," while keeping environment
separation as a structural property (separate files, separate deploy
targets) rather than a runtime `if environment == "prod"` branch scattered
through pipeline code.

## Trade-offs

- A business-metadata change (e.g. a new DQ rule) affects every
  environment simultaneously once merged — there's no way to test a DQ
  rule change in Dev-only config first; it must be tested against Dev
  *data* with the same rule active everywhere, which this project
  considers the correct trade-off (representative Dev testing) but is
  worth naming explicitly.
- `ConfigLoader`'s deep-merge behavior (Section 25/LLD-08) means the
  order base YAML files are loaded in is significant — a subtle correctness
  dependency that isn't obvious from reading any single file in isolation.
- Environment-specific values still require a real secret-management
  answer (Secrets Manager) as a separate concern — this ADR only covers
  non-secret configuration; secrets follow their own resolution path
  (ADR referenced in the security HLD, not a separate ADR in this set since
  it's a straightforward "never do X" rule rather than a decision with real
  alternatives).
