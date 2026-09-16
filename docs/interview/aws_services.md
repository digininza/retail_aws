# AWS Services — Purpose and Boundaries

One-line justification plus the explicit boundary of what each service does
**not** do in this platform, so the "why not use X for everything" question
has a ready answer.

| Service | Purpose | Explicit boundary |
|---|---|---|
| **Amazon S3** | Durable, zoned data lake (raw/bronze/silver/gold/quarantine/archive). | Not a query engine — access requires Glue/Athena/Redshift Spectrum on top. |
| **AWS Glue** | Managed batch ingestion, standard ETL, Data Catalog, partition management, schema validation. | Not used for TB-scale complex joins or workloads needing hand-tuned Spark cluster control (that's EMR). |
| **Amazon EMR** | Heavy/complex Spark: large historical joins, TB-scale aggregation, backfills. | Not used for lightweight per-table cleansing — spinning up a cluster for a small incremental load wastes cost and time (that's Glue). |
| **Apache Airflow (MWAA)** | Orchestration: scheduling, dependencies, retries, backfill parameterization. | Never executes a Spark transformation itself — every heavy task is triggered-and-monitored, not run inline. |
| **AWS Lambda** | Lightweight, event-driven glue: file-arrival triggers, streaming record validation, single API calls, pipeline triggering. | Never used for TB-scale or even GB-scale bulk transformation — timeout and memory limits make it the wrong tool, and Section 17 explicitly rules this out. |
| **Amazon Kinesis** | Real-time ingestion of POS/e-commerce events, shard-partitioned for intra-entity ordering. | Not a long-term store — records are consumed and landed in S3 promptly, not retained indefinitely in the stream. |
| **Amazon Redshift** | Curated analytical warehouse: dimensional model (dims + facts + marts) for BI. | Not used as an operational/transactional database, and not the target of direct source-system writes — everything arrives via staged, reconciled loads. |
| **IAM** | Least-privilege access control, one role per service per environment. | Not a substitute for network-level controls or encryption — complementary, not sufficient alone. |
| **KMS** | Encryption at rest for S3, Redshift, and Secrets Manager, via per-environment CMKs. | Doesn't manage in-transit encryption on its own — that's TLS enforcement via bucket policy. |
| **Secrets Manager** | Resolves source-system credentials/API keys at runtime by ARN reference. | Never stores business configuration or pipeline logic — that stays in version-controlled YAML. |
| **CloudWatch** | Logs, custom metrics (records read/written/rejected, DQ failures, duration), alarms. | Doesn't itself notify anyone — alarms trigger SNS, which does the notifying. |
| **SNS** | Fan-out notification on pipeline failure, DQ CRITICAL failure, reconciliation failure. | Not a queue for reliable processing — it's a notification/alerting mechanism, not a work-distribution mechanism (that would be SQS, not used in this platform's design). |
| **AWS CodeBuild / CodePipeline** | CI/CD: validate, test, package, and deploy across Dev → QA → Prod with a manual Prod gate. | The YAML pipeline/build definitions don't execute anything themselves — CodeBuild/CodePipeline are the services that do (Section 26). |

## "Why not just use X for everything" — quick answers

- **Why not EMR for everything?** Wastes cost/time on small, frequent jobs;
  Glue's managed model is a better fit for those. See ADR-002.
- **Why not Glue for everything?** Under-powered for hand-tuned, heavy
  Spark workloads with explicit partitioning/broadcast/skew control needs.
  See ADR-002.
- **Why not Airflow as the Spark engine?** It's a shared, comparatively
  small orchestration resource — making it a data-processing bottleneck
  defeats the purpose of having dedicated Glue/EMR compute. See ADR-003.
- **Why not S3 + Athena only, skip Redshift?** Works for ad hoc queries;
  lacks the workload-managed concurrency and native MERGE semantics this
  platform's SCD2/fact-load patterns depend on. See ADR-008.
- **Why not Lambda for the bulk transformations?** Timeout/memory limits
  and cost-per-invocation model make it wrong for anything beyond
  lightweight, fast, event-driven logic. See Section 17.
