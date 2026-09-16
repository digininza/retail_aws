# ADR-004: Watermark / Control-Table Strategy

## Context

Incremental pipelines need to know "where did I leave off," reliably
surviving failures without either reprocessing everything (wasteful, and
risks duplicate side effects) or silently skipping data (data loss). This
state must be queryable/auditable, and must never advance ahead of what has
actually been successfully processed downstream (Section 7).

## Decision

Maintain an explicit relational control table (`control.pipeline_control`,
one row per pipeline) as the single source of truth for
`last_successful_watermark`, updated **only** after extract → write → DQ →
reconciliation → commit have all succeeded for that run
(`src.common.idempotency.WatermarkCommitGuard`). Reference implementation
uses SQLite locally / a Redshift or RDS control schema in AWS.

## Alternatives considered

- **AWS Glue Job Bookmarks alone** — rejected as the sole mechanism: opaque
  (no queryable history), scoped to one Glue job definition (doesn't
  generalize if the same table is later touched by EMR or Lambda), and
  cannot express this project's required stage ordering. Retained as a
  secondary, complementary optimization only (see LLD-01).
- **DynamoDB control table** — a valid alternative, especially at very high
  pipeline-count scale where DynamoDB's request-based pricing and
  single-digit-millisecond reads outperform a relational table under heavy
  concurrent access. Not chosen for the reference implementation because
  this project's control table is read/written once or a few times per
  pipeline run (low request volume), where a relational table's
  transactional guarantees and SQL queryability (for ad hoc
  troubleshooting: "show me every pipeline whose watermark hasn't advanced
  in 24 hours") are a better fit than DynamoDB's eventually-consistent,
  key-value-first model.
- **No control table — infer state from S3 object listing** — rejected:
  fragile (what if a raw file was manually deleted or duplicated?), and
  loses the audit narrative (who/when/how many records) entirely.

## Rationale

A dedicated control table, separate from the data lake itself, means:
reprocessing/deleting lake data never destroys operational history;
watermark commit timing can be made an explicit, testable state machine
(`RunStage` enum) rather than an implicit convention; and the table is
directly queryable for operational questions without needing to parse S3
prefixes.

## Trade-offs

- One more piece of infrastructure to run/back up/secure, versus inferring
  state from the lake alone.
- A relational control table is a single point of write contention if many
  pipelines run concurrently at very high frequency — mitigated here by
  one row per pipeline (row-level, not table-level, contention) and by the
  reference implementation's scale (this is not designed for
  thousands of pipelines running every minute).
- Requires disciplined code review to ensure every new pipeline's write
  path actually goes through `WatermarkCommitGuard` rather than updating
  the watermark ad hoc — the safety property is enforced by convention +
  the guard's API design (advancing stages is the only sanctioned path to
  `resolved_watermark()` returning the new value), not by a database
  constraint.
