# Senior-Level Discussion Points

For a conversation that goes beyond "how did you build X" into "how do you
think about trade-offs" — the questions a Staff/Principal-leaning
interviewer or a design-review panel is more likely to ask.

## Architecture decisions and trade-offs

- **Every ADR in this repo names alternatives that were rejected and why**
  (`docs/adr/`) — not just what was chosen. Be ready to argue the *other*
  side: e.g., "when would EMR-for-everything actually be the right call?"
  (Answer: an org with a much higher ratio of heavy-to-light workloads,
  where Glue's per-job overhead dominates.)
- **The Glue/EMR/Airflow boundary is a discipline, not a technical wall.**
  Nothing stops a developer from writing Spark inside an Airflow
  `PythonOperator`. The boundary is enforced by code review and
  documented convention (Section 13, ADR-003). A senior discussion point:
  how would you make this boundary *harder* to violate — e.g., a CI lint
  rule that flags `pyspark` imports inside `airflow/dags/`?

## Reliability and correctness

- **Idempotency is the load-bearing property, not retry logic itself.**
  Retries are safe *because* every write path (`merge_upsert`, SCD2 apply,
  watermark commit) is idempotent — not because retry code is clever. This
  is the single most important design property in the repo, and it's worth
  being able to explain precisely why (composite idempotency keys, never a
  bare timestamp; matched/unmatched merge semantics; hash-based no-op
  detection).
- **The `WatermarkCommitGuard` state machine is deliberately restrictive**
  — it doesn't just document "advance the watermark last," it makes
  advancing it early a `ValueError` at the code level (illegal stage
  transition). This is an example of making a correctness property
  structural rather than just documented — a good pattern to generalize
  in a discussion about "how do you prevent regressions in critical
  invariants."
- **What's honestly NOT implemented**, and why that's a defensible
  scoping decision for a reference project rather than a real gap:
  exactly-once streaming semantics, alert deduplication/suppression,
  point-in-time SCD2 fact joins (the reference `fact_sales` builder uses
  current-snapshot joins for readability, with the point-in-time approach
  documented but not implemented), multi-writer SCD2 concurrency control
  beyond `max_active_runs=1`.

## Security posture

- **Least privilege by S3 *prefix*, not just bucket** — a Lambda function
  that only needs to read `raw/` and write `quarantine/` gets exactly that,
  not "the data bucket." Worth discussing: how this scales when the number
  of functions/roles grows — at what point does per-function role
  proliferation itself become an operational burden, and what's the
  mitigation (role templates, permission boundaries)?
- **Prod's manual approval gate is a deliberate velocity-vs-safety
  trade-off**, not a default. Worth being able to argue when you'd remove
  it (e.g., a mature test suite with high confidence + feature-flagged
  rollout + automated rollback) versus when you'd keep it (low deploy
  frequency, high consequence of a bad Prod data change, regulatory
  requirements).

## Scalability and cost

- **Ephemeral EMR clusters vs. a standing cluster** is a real cost/latency
  trade-off, not a free lunch — cluster startup time (minutes) is a cost
  paid on every backfill run. At what backfill frequency would a standing
  (or auto-scaling) cluster start winning? That's a legitimate follow-up
  question to be ready for.
- **The control table's relational design doesn't scale indefinitely** —
  it's the right choice at this project's pipeline count and run
  frequency (ADR-004 names DynamoDB explicitly as the alternative for much
  higher concurrent-write scale). Knowing *where* that crossover point
  roughly is (order of magnitude: hundreds of pipelines running every few
  minutes vs. dozens running daily) shows real judgment, not just
  familiarity with both options.

## Ownership and operational maturity

- **Every failure category has a named detection mechanism, retry
  decision, and alert path** (`docs/lld/10_failure_handling_lld.md`'s
  failure matrix) — this is the artifact a senior engineer should be able
  to produce for *any* system they own, not just this one. It's a good
  format to reuse in a live interview if asked "walk me through what
  happens when X breaks."
- **Audit/observability is designed for "what happened" questions to be
  answerable without reading code** — `run_id`, environment, counts,
  watermarks before/after, error category, all in one structured log line
  and one audit table row per run (Section 36). This is the difference
  between a system you can operate and one you can only debug by reading
  source.

## Questions worth asking back in an interview

If given the chance to ask the interviewer something about *their* system
after walking through this project, strong signal comes from asking:
"Where does your platform draw the Glue/EMR-equivalent boundary?", "How do
you currently guarantee a retry doesn't duplicate a side effect?", and "What's
your reconciliation story, separate from data quality?" — these are the
questions this project's design directly answers for itself, and asking
them shows the same instinct applied to someone else's system.
