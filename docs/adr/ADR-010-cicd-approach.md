# ADR-010: CI/CD Approach

## Context

Code, configuration, and infrastructure changes need a repeatable,
reviewable path from a developer's commit to running in Prod, with
automated validation catching regressions before they reach customer-
adjacent (Prod) data, while not making every Dev iteration painfully slow.

## Decision

AWS CodeBuild (validate/test/package) + AWS CodePipeline (Source → Build →
Deploy Dev → Smoke Test → Deploy QA → Manual Approval → Deploy Prod → Post-
deployment Validation), per Section 26. Dev and QA deploy automatically on
every merge to `main`; Prod requires an explicit human approval.

## Alternatives considered

- **GitHub Actions for build/test, CodePipeline only for deploy** — a
  reasonable split many teams use; not chosen for this reference project
  to keep the full toolchain AWS-native and consistent with the project's
  AWS-centric scope, and because CodeBuild's tight IAM-role integration
  simplifies the deployment role story (ADR/IAM section) versus managing
  OIDC federation from an external CI system.
- **Auto-deploy to Prod with no manual gate, relying entirely on automated
  tests** — rejected: this platform's test suite (unit/integration) proves
  logical correctness against sample/local data, not full-scale behavior
  against live Prod source systems; a human checkpoint before Prod is a
  deliberate, cheap insurance policy given the asymmetric cost of a bad
  Prod deploy (Section 26 stage 8).
- **Separate deployment tooling per artifact type (Terraform for infra,
  Serverless Framework for Lambda, a custom script for Glue)** — rejected
  for this project's scale: a unified CodeBuild/CodePipeline flow, driving
  CloudFormation + AWS CLI sync commands, keeps one mental model for "how
  does a change get to Prod" rather than several tool-specific ones.

## Rationale

The pipeline stage sequence mirrors standard promotion discipline
(validate → test → package → deploy-with-increasing-blast-radius) and
makes the "what actually executes a deployment" question unambiguous:
YAML (`buildspec.yml`, `pipeline.yaml`) declares structure and
configuration; CodeBuild/CodePipeline execute it (Section 26's explicit
requirement not to conflate the two).

## Trade-offs

- Prod deploys are gated by human availability (someone must click
  approve) — a deliberate trade of deploy velocity for safety, appropriate
  given this platform's low Prod deploy frequency (infrequent, high-
  consequence changes) rather than a high-frequency microservice deploy
  cadence.
- No automated rollback strategy is implemented in this reference
  repository beyond CloudFormation's own stack rollback-on-failure
  behavior — a production system would likely add explicit
  blue/green or canary deployment for Glue job version changes, out of
  scope here.
- The pipeline validates code/config correctness, not full pipeline
  *behavioral* correctness against live source systems — smoke testing
  (Dev only) is the closest approximation this reference implementation
  provides, and is explicitly scoped to one low-risk pipeline, not full
  coverage.
