# ADR-006: SCD Type 2 for dim_customer

## Context

Analytics need to answer questions like "what segment was this customer in
when they placed this order" — which requires knowing the customer's
attributes *as of* the order date, not only their current attributes. Only
some dimensions have this requirement; applying full history-tracking
everywhere would add unjustified complexity.

## Decision

Apply SCD Type 2 (full history, surrogate key per version) to
`dim_customer` only. `dim_product`, `dim_store`, `dim_promotion` use Type 1
(overwrite, no history) (Section 10).

## Alternatives considered

- **SCD Type 2 for every dimension** — rejected: `dim_store` and
  `dim_promotion` change rarely and their historical values are not
  typically analytically meaningful for this project's use cases (a
  renamed store doesn't need "as of" fact attribution the way a customer's
  segment does); the added surrogate-key/effective-date complexity isn't
  earned.
- **SCD Type 1 for dim_customer (overwrite)** — rejected: loses the exact
  capability the business needs — segment-at-time-of-purchase analysis —
  which is the primary reason `dim_customer` exists as a dimension at all
  in this platform's analytical model.
- **SCD Type 3 (previous-value column) for dim_customer** — rejected:
  only preserves one prior version, insufficient for a customer with
  multiple attribute changes over their lifetime (e.g. moved cities twice).

## Rationale

Hash-based change detection (`record_hash` over `TRACKED_ATTRIBUTES`) keeps
the "did anything I care about change" question cheap and easy to extend
(adding a tracked attribute is a one-line list change, not a rewrite of
comparison logic — see LLD-03). Surrogate keys decouple "a specific version
of a customer" from "the customer's stable identity," which is exactly
what a point-in-time fact join needs.

## Trade-offs

- SCD2 tables grow monotonically (a new row per change, never
  overwritten) — requires the retention/lifecycle thinking documented in
  Section 5/34, not implemented as automatic pruning in this reference
  repo.
- Fact-to-dimension joins that want "as of" semantics must join on
  `effective_start_date <= order_date < effective_end_date`, not a simple
  equi-join on `customer_id` — `src/dimensional_model/fact_sales.py`'s
  reference implementation deliberately uses the simpler *current*-snapshot
  join and documents the point-in-time variant as the production-correct
  approach not implemented here, to keep the demo readable (see that
  module's docstring).
- Concurrency: only single-writer SCD2 merges are proven safe in this
  reference implementation (see LLD-03's concurrency note and
  `customer_pipeline`'s `max_active_runs=1`).
