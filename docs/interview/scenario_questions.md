# Scenario-Based Interview Questions

Each answer follows **Problem → Design → Implementation → Trade-off →
Result**. References point to the actual code/docs in this repo.

---

### 1. Why Glue and EMR together?

**Problem:** Workloads range from small per-table incremental loads to
TB-scale historical joins — one engine can't serve both efficiently.
**Design:** Route by workload shape (`config/base/pipelines.yaml:
engine_routing`) — Glue for standard ETL, EMR for heavy/complex Spark.
**Implementation:** `glue/jobs/*.py` for extraction/cleansing;
`emr/jobs/*.py` for historical backfill, customer 360, inventory rollups.
**Trade-off:** Two operational surfaces to secure/monitor instead of one.
**Result:** Neither workload pays for the other's overhead. See ADR-002.

### 2. Why not use Glue for everything?

**Problem:** Heavy historical joins need explicit shuffle/broadcast/skew
control Glue's managed model abstracts away.
**Design:** Reserve EMR for exactly those workloads.
**Implementation:** `emr/jobs/large_sales_transformation.py` broadcasts
small dimensions explicitly, salts a skewed join key in
`inventory_aggregation.py`.
**Trade-off:** More infrastructure to run.
**Result:** Backfills complete predictably instead of fighting Glue's
managed-job constraints.

### 3. Why Airflow if Glue can schedule jobs?

**Problem:** Pipelines span Glue *and* EMR *and* Python-level
reconciliation logic — no single engine's native scheduler covers all of it.
**Design:** Airflow owns cross-engine dependency graphs, retries, SLAs.
**Implementation:** `airflow/dags/sales_pipeline.py`'s
Glue → EMR → Reconciliation → Redshift sequence.
**Trade-off:** Another system to operate (MWAA).
**Result:** One place to see whether today's pipeline actually succeeded,
regardless of which engine ran which step. See ADR-003.

### 4. Why S3 and Redshift?

**Problem:** Need cheap, durable, replayable raw storage *and*
fast, concurrent, SQL-native analytical access — one technology doesn't do
both well.
**Design:** S3 as the lake (storage/compute decoupled), Redshift as the
curated warehouse (compute-optimized, MERGE-capable).
**Implementation:** Gold zone → Redshift staging → MERGE into curated.
**Trade-off:** Data exists in two places conceptually — lake and warehouse
— requiring a defined promotion boundary.
**Result:** Cheap long-term raw retention plus fast BI queries, each on the
right-shaped technology. See ADR-001, ADR-008.

### 5. Where does the incremental control table live?

**Problem:** Watermark state must be queryable, durable, and separate from
the data it governs.
**Design:** A dedicated relational control table
(`control.pipeline_control`), not inferred from S3 listings.
**Implementation:** `sql/control_tables/ddl_control_table.sql`,
`src/common/audit.py::ControlTableStore`.
**Trade-off:** One more piece of infrastructure to run/secure.
**Result:** Reprocessing/deleting lake data never destroys operational
history. See ADR-004.

### 6. When is the watermark updated?

**Problem:** Updating too early risks silently skipping data on a later
failure.
**Design:** Only after extract → write → DQ → reconciliation → commit all
succeed.
**Implementation:** `WatermarkCommitGuard` — `resolved_watermark()` only
returns the new value when `stage == COMMITTED`.
**Trade-off:** A failed run always reprocesses the same window on retry —
by design, since downstream writes are idempotent.
**Result:** No silent data loss on partial failure. See Section 7, ADR-004.

### 7. What happens if EMR fails after S3 ingestion?

**Problem:** A cluster failure shouldn't corrupt or skip data.
**Design:** The control table's watermark is untouched until the full
chain (including EMR's output) passes reconciliation.
**Implementation:** `sales_pipeline.py`'s `emr_monitor` sensor; guard stays
below `COMMITTED`.
**Trade-off:** The next run/backfill reprocesses the same window (EMR time
cost) rather than resuming mid-transform.
**Result:** Correctness preserved over granular resumability — an
acceptable trade given backfills are infrequent.

### 8. How do you restart without reprocessing everything?

**Problem:** A full reprocess on every retry is wasteful.
**Design:** The control table's `last_successful_watermark` is the
resumption point, always.
**Implementation:** Every extractor reads this value before pulling
changes (`extract_incremental(..., since=previous_watermark)`).
**Trade-off:** Only the *last committed* point is resumable, not
finer-grained mid-pipeline checkpoints.
**Result:** A rerun only reprocesses what was never successfully committed.

### 9. How do you make the pipeline idempotent?

**Problem:** Retries must not create duplicate downstream effects.
**Design:** Composite idempotency keys (never a bare timestamp) + merge/
upsert keyed on business identity.
**Implementation:** `src.common.idempotency.build_idempotency_key`,
`src.cdc.merge.merge_upsert`.
**Trade-off:** Requires every pipeline to define a real business key —
can't shortcut with an auto-increment/load-timestamp key.
**Result:** Same input, same run outcome, every time. See Section 9.

### 10. How do you implement MERGE/upsert?

**Problem:** Need insert-if-absent/update-if-present without duplicating on
rerun.
**Design:** Key-based matched/unmatched branching, generic across
pipelines.
**Implementation:** `src/cdc/merge.py::merge_upsert`;
`sql/facts/merge_fact_sales.sql` (Redshift-native `MERGE`).
**Trade-off:** Row-by-row Python loop in the reference implementation
doesn't scale to TB-size batches (fine for CDC batch sizes; the SQL
`MERGE` handles the large fact-table case).
**Result:** One reusable, tested implementation instead of per-pipeline
reinvention. See LLD-06.

### 11. How do you handle deletes?

**Problem:** CDC deletes must remove exactly the right row, once.
**Design:** Deletes applied first, keyed on business key, before
updates/inserts in the same batch.
**Implementation:** `merge_upsert`'s delete branch.
**Trade-off:** A delete for a key not present in target is a silent no-op,
not an error — acceptable since replay/rerun makes this a normal case.
**Result:** Deterministic, order-independent-within-a-batch delete
handling.

### 12. How do you implement SCD2?

**Problem:** Need full attribute history without duplicate versions on
rerun.
**Design:** Hash-based change detection; expire-then-insert on change,
no-op on match.
**Implementation:** `src/dimensional_model/scd_type2.py::apply_scd2`;
`sql/dimensions/scd2_merge_dim_customer.sql`.
**Trade-off:** Only applied to `dim_customer` — not every dimension gets
this complexity.
**Result:** Point-in-time customer attribution possible for analytics.
See ADR-006, LLD-03.

### 13. How do you handle duplicate CDC events?

**Problem:** A redelivered or genuinely-duplicated CDC event shouldn't be
applied twice.
**Design:** Dedup by business key + latest `change_timestamp` before any
apply.
**Implementation:** `src/cdc/processor.py::deduplicate_cdc_batch`.
**Trade-off:** Relies on `change_timestamp` accuracy, not a stronger
ordering signal like an LSN.
**Result:** Exact duplicates collapse to one application, deterministically.

### 14. How do you handle late-arriving events?

**Problem:** An event can arrive after the watermark has already advanced
past its business timestamp.
**Design:** Flag (not drop) late events; downstream aggregates decide
inclusion.
**Implementation:** `src.ingestion.streaming.event_schema.is_late_event`.
**Trade-off:** Not automatically re-triggering already-closed downstream
aggregates — that's a documented gap, not implemented.
**Result:** Late data isn't silently lost, even if perfect reconciliation
of an already-closed window isn't automatic.

### 15. How do you handle schema evolution?

**Problem:** Source schema changes shouldn't silently corrupt or crash the
pipeline.
**Design:** Classify every change as COMPATIBLE/WARNING/BREAKING; BREAKING
halts the pipeline immediately.
**Implementation:** `src/data_quality/validators.py::classify_schema_change`.
**Trade-off:** Requires a maintained schema registry
(`config/base/schemas.yaml`) as a source of truth.
**Result:** A renamed/removed required column fails fast and alerts,
instead of silently nulling data. See Section 24.

### 16. How do you stop bad data from reaching Gold?

**Problem:** Invalid records must never reach the analytics-facing layer.
**Design:** CRITICAL-severity DQ rules block promotion entirely.
**Implementation:** `src/data_quality/framework.py::enforce` raises
`DataQualityError`.
**Trade-off:** A single CRITICAL failure in a batch of otherwise-good
records blocks the whole batch (ERROR-severity failures are more granular:
only the failing rows are quarantined).
**Result:** Gold is a trusted layer by construction, not by hope.

### 17. How does reconciliation differ from DQ?

**Problem:** A batch can be individually valid but volumetrically wrong (or
vice versa) — one check doesn't catch both.
**Design:** Two independent gates: DQ = "is each record valid," 
reconciliation = "did the expected data actually move."
**Implementation:** `src/data_quality/framework.py` vs.
`src/reconciliation/framework.py`.
**Trade-off:** Two systems to configure and maintain per pipeline.
**Result:** Silent data loss (good records that just didn't all arrive) is
caught even when every arriving record passes DQ. See HLD-04.

### 18. How do you handle API throttling?

**Problem:** A REST source rate-limiting the platform shouldn't fail the
pipeline outright.
**Design:** `ApiRateLimitError` is retryable, with backoff that also
respects the configured rate limit.
**Implementation:** `src/ingestion/rest_api/client.py::RestApiClient`.
**Trade-off:** Slower ingestion under sustained throttling — accepted over
failing the run.
**Result:** Transient throttling resolves itself without manual
intervention, up to `max_attempts`.

### 19. How do you handle source system downtime?

**Problem:** A temporarily unreachable source shouldn't corrupt state.
**Design:** `SourceUnavailableError` (retryable) with exponential backoff;
watermark stays frozen on exhaustion.
**Implementation:** `src/ingestion/sqlserver/extractor.py`,
`src.common.retry`.
**Trade-off:** Repeated downtime means repeated alert noise (no dedup
implemented — documented gap).
**Result:** No data loss, no partial/corrupt extract; clean resumption once
the source is back.

### 20. How do Airflow retries work?

**Problem:** Task-level failures need automatic recovery without operator
intervention for transient issues.
**Design:** Per-DAG `retries`/`retry_delay`(+backoff), layered on top of
each job's own internal retry logic.
**Implementation:** `default_args` in every `airflow/dags/*.py`.
**Trade-off:** Two retry layers (Airflow + internal) can mask which layer
actually resolved a transient issue if not logged distinctly.
**Result:** Most transient failures self-heal without paging anyone.
See LLD-07.

### 21. What should and should not be retried?

**Problem:** Retrying a business-rule/data failure indefinitely wastes time
and delays the real alert.
**Design:** `RetryableError` (transient: connectivity, throttling) vs.
`NonRetryableError` (DQ, reconciliation, schema, config) — a hard
type-level split.
**Implementation:** `src/common/exceptions.py`, enforced in
`src.common.retry.with_retry`.
**Trade-off:** Every new exception type must be deliberately classified —
miscategorizing one silently changes its retry behavior.
**Result:** No infinite-retry loops on data problems that retrying can't
fix. See Section 22.

### 22. How do you alert on failures?

**Problem:** Failures need to reach a human promptly, with enough context
to act.
**Design:** SNS publish on every pipeline failure, DQ CRITICAL failure,
and reconciliation failure, from both the pipeline code and Airflow's
`on_failure_callback`.
**Implementation:** `src.common.utilities.SNSNotifier`,
CloudWatch alarms in `infrastructure/monitoring/`.
**Trade-off:** No alert deduplication/suppression implemented — repeated
failures page repeatedly.
**Result:** An on-call engineer gets `run_id`, `pipeline`, and error
category immediately, without digging through logs first.

### 23. How do you secure S3?

**Problem:** The data lake holds PII and business-sensitive data; broad
access is unacceptable.
**Design:** Bucket policy denies non-TLS and non-KMS-encrypted writes;
explicit principal allow-list; per-prefix IAM scoping.
**Implementation:** `infrastructure/s3/bucket-policy.json`,
`infrastructure/iam/*-execution-role-policy.json`.
**Trade-off:** More granular IAM policies to maintain than one broad
"data-platform" role.
**Result:** A compromised Lambda role, for example, can't read the whole
lake — only its function's specific prefix.

### 24. What IAM roles are required?

**Problem:** Each AWS service needs exactly the access it needs, no more.
**Design:** One role per service per environment (Glue, EMR, Lambda [per
function], MWAA, Redshift-load, CI/CD deployment).
**Implementation:** `infrastructure/iam/README.md`'s role matrix.
**Trade-off:** More roles to provision/audit than a shared role.
**Result:** A clear, auditable answer to "who can access what and why" —
see Section 19.

### 25. Why use Secrets Manager?

**Problem:** Hardcoded or environment-variable-committed credentials are a
standing security liability.
**Design:** Every source credential resolved by ARN reference at runtime,
never stored in code or YAML.
**Implementation:** `src.common.utilities.SecretsManagerAdapter`,
`.env.example`'s documented placeholder pattern.
**Trade-off:** Requires provisioning/rotating secrets as real
infrastructure, not just config edits.
**Result:** Credential rotation doesn't require a code deploy; nothing
sensitive is ever in version control.

### 26. Where is KMS used?

**Problem:** Data at rest must be encrypted with keys the platform
controls and can audit/rotate.
**Design:** Per-environment CMKs on S3 buckets, Redshift clusters, and
Secrets Manager.
**Implementation:** `config/{env}/environment.yaml: s3.kms_key_alias`,
`infrastructure/cloudformation/s3-data-lake.yaml`.
**Trade-off:** Cross-environment data copying (if ever needed) requires
explicit key-grant handling — not just an S3 copy.
**Result:** Encryption at rest is enforced structurally (bucket policy
denies non-KMS writes), not just configured and hoped-for.

### 27. How do you separate Dev/QA/Prod?

**Problem:** A Dev bug must never be able to touch Prod data.
**Design:** Fully separate AWS accounts, buckets, IAM roles, and clusters
per environment; no shared "default" resource.
**Implementation:** `config/{dev,qa,prod}/environment.yaml`; CI/CD's
per-environment `sts:AssumeRole`.
**Trade-off:** Three times the infrastructure to provision/monitor versus
one shared environment with logical separation.
**Result:** Blast radius of any single environment's issue is structurally
contained. See ADR-009.

### 28. How do YAML configurations work?

**Problem:** Business metadata shouldn't be duplicated per environment;
environment-specific values shouldn't leak into business logic.
**Design:** Base (environment-agnostic) + per-environment overlay, deep-merged.
**Implementation:** `src.common.config_loader.ConfigLoader`.
**Trade-off:** File load order in `_base()` is a real (if minor)
correctness dependency.
**Result:** Onboarding a table is one YAML addition to `sources.yaml`, not
a change replicated three times. See LLD-08.

### 29. What exactly does CI/CD deploy?

**Problem:** "Deploy" needs a precise, unambiguous definition, not a vague
concept.
**Design:** Glue/EMR scripts, Lambda code, Airflow DAGs/plugins, SQL, and
CloudFormation-managed infrastructure — enumerated explicitly, nothing
implicit.
**Implementation:** `cicd/buildspec.yml`'s `post_build` packaging step,
`cicd/scripts/deploy.sh`.
**Trade-off:** Every new artifact type needs an explicit line added to the
packaging/deploy scripts — nothing deploys "automatically" by convention.
**Result:** No ambiguity about what a given pipeline run actually changed
in an environment. See Section 26.

### 30. How do you prevent production configuration from being overwritten?

**Problem:** An accidental or unreviewed config change shouldn't reach Prod
silently.
**Design:** `config/prod/environment.yaml` changes go through the exact
same PR review + CI/CD approval gate as code changes — no separate,
lower-friction config-only path.
**Implementation:** `cicd/pipeline.yaml`'s `ManualApprovalForProd` stage
applies regardless of whether the diff is code or config.
**Trade-off:** Even a trivial config typo fix requires the full pipeline
path.
**Result:** No "just this once" backdoor for Prod configuration changes.

### 31. How do you optimize an EMR Spark job?

**Problem:** Naive Spark code on large data leads to shuffle bottlenecks
and long runtimes.
**Design:** Explicit broadcast joins for small dimensions, column pruning
before joins, partitioning aligned to aggregation keys, AQE enabled.
**Implementation:** `emr/jobs/historical_backfill.py`,
`emr/jobs/large_sales_transformation.py`.
**Trade-off:** Requires understanding the data's actual size distribution
— a wrong broadcast-join guess (broadcasting something too large) causes
executor OOMs.
**Result:** Predictable runtimes instead of jobs that occasionally hang on
a skewed shuffle. See Section 15.

### 32. How do you identify data skew?

**Problem:** A few keys with disproportionate row counts (e.g. flagship
stores' bestselling SKUs) can bottleneck a single reducer task.
**Design:** Salting the skewed join key to spread it across synthetic
partitions.
**Implementation:** `emr/jobs/inventory_aggregation.py::_salted_join`.
**Trade-off:** Salting adds a join-key transformation and a slightly larger
intermediate dataset (N synthetic buckets per skewed key).
**Result:** No single task processes a disproportionate share of the data.

### 33. When would you use broadcast join?

**Problem:** A shuffle (sort-merge) join between a large and small table
wastes network/disk shuffling the large side unnecessarily.
**Design:** Broadcast the small side explicitly when it reliably fits in
executor memory.
**Implementation:** `F.broadcast(dim_store)`, `F.broadcast(dim_date)` in
`large_sales_transformation.py`.
**Trade-off:** Broadcasting a table that turns out to be larger than
expected OOMs every executor — must be a deliberate, verified decision,
not just relying on Spark's auto-broadcast threshold.
**Result:** Dimension joins add negligible shuffle cost to a large fact
join.

### 34. How do you handle small files?

**Problem:** Many small output files degrade both write and downstream
read performance.
**Design:** Coalesce before write; let AQE coalesce shuffle partitions
dynamically instead of a fixed high partition count.
**Implementation:** `.coalesce(2)` in `historical_backfill.py`;
`spark.sql.adaptive.coalescePartitions.enabled`.
**Trade-off:** Coalescing to too few partitions can reduce write
parallelism — a balance, not a one-size number.
**Result:** Output file sizes stay in a healthy range for downstream
readers.

### 35. How do you process TB-scale historical data?

**Problem:** A full historical backfill is too large for Glue's managed
model to tune effectively.
**Design:** Ephemeral, purpose-sized EMR cluster; explicit partitioning by
year/month; broadcast joins for dimensions.
**Implementation:** `emr/jobs/historical_backfill.py`,
`airflow/dags/sales_pipeline.py`.
**Trade-off:** Cluster provisioning time adds latency versus an always-on
option — accepted for cost reasons (Section 34).
**Result:** A workload that runs occasionally gets right-sized compute
each time, not a permanently oversized standing cluster.

### 36. How do you monitor pipeline health?

**Problem:** Need to know a pipeline is unhealthy before a business user
notices a stale/wrong report.
**Design:** Structured logging with `run_id`/counts/status on every stage;
CloudWatch alarms on failures, DQ/reconciliation failures, freshness, and
duration SLA breaches.
**Implementation:** `src.common.logging_utils`,
`infrastructure/monitoring/cloudwatch-alarms.json`.
**Trade-off:** More alarms to tune thresholds for and avoid alert fatigue.
**Result:** Operational visibility (Section 20) without needing to
manually query the control/audit tables to know something's wrong.

### 37. How do you backfill historical data?

**Problem:** Reprocessing a historical window shouldn't require a
different code path than normal runs.
**Design:** Backfill is a `dag_run.conf` parameter
(`start_date`/`end_date`), not a separate DAG or pipeline.
**Implementation:** `airflow/dags/sales_pipeline.py`.
**Trade-off:** Backfill windows aren't automatically validated against
gaps in already-processed history — an operator specifies the window
deliberately.
**Result:** One tested, reviewed pipeline definition serves both the daily
schedule and ad hoc historical reprocessing.

### 38. How do you prevent duplicate Redshift loads?

**Problem:** A rerun of the same staging batch shouldn't double-count
sales.
**Design:** `MERGE` keyed on the composite business key
(`order_id, order_item_id`), not append-only `INSERT`.
**Implementation:** `sql/facts/merge_fact_sales.sql`.
**Trade-off:** `MERGE` is more complex to write/review than a plain
`COPY` + `INSERT`, and requires staging to always represent "this run's
data," never an accumulating append.
**Result:** Rerunning the exact same staging load twice leaves
`fact_sales` in the same state both times.

### 39. How do you handle partial failures?

**Problem:** A pipeline that fails partway through shouldn't leave
downstream systems in an inconsistent, ambiguous state.
**Design:** Staged commit — writes go to a staging/temporary location
first; only promoted to the final target after DQ + reconciliation pass;
watermark reflects only fully-committed state.
**Implementation:** `WatermarkCommitGuard`, `sql/staging/*`.
**Trade-off:** A partial failure still means wasted extract/transform work
that must be redone on retry — no fine-grained mid-pipeline resumability.
**Result:** Downstream consumers only ever see fully-validated,
fully-reconciled data — never a half-loaded batch. See Section 23.

### 40. What was the most difficult production issue?

*(This is a fictional/portfolio project — answered here as the kind of
issue this architecture is specifically designed to prevent, framed as a
hypothetical.)* **Problem:** A pipeline that appeared to succeed (green in
Airflow) but had silently lost 8% of records between Glue's bronze and
silver stages due to an unintentional inner-join filtering out valid rows
with a null optional foreign key. **Design:** This is exactly what
reconciliation (not just DQ) exists to catch — the records that were
present were individually valid; the *count* was wrong.
**Implementation:** A `RECORD_COUNT` reconciliation check with
`tolerance_pct: 0` on that pipeline would have failed the run instead of
silently promoting fewer records than were extracted. **Trade-off:** This
means every pipeline needs a genuine reconciliation profile, not just DQ
rules — an easy thing to skip when a pipeline "seems simple." **Result:**
The architectural lesson baked into this project: DQ alone is not enough;
reconciliation is a mandatory, separate gate for exactly this class of
silent-data-loss bug.
