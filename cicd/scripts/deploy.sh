#!/bin/bash
# Deploys the packaged artifact (Glue scripts, Lambda code, Airflow DAGs,
# config, infrastructure templates) to the target environment. Invoked by
# the corresponding CodePipeline "Deploy{Env}" stage action, parameterized by
# cicd/deployment/{env}.yaml. Not intended to be run manually against Prod.
set -euo pipefail

ENVIRONMENT="${1:?Usage: deploy.sh <dev|qa|prod>}"
DEPLOY_CONFIG="cicd/deployment/${ENVIRONMENT}.yaml"

if [[ ! -f "$DEPLOY_CONFIG" ]]; then
  echo "Unknown environment '${ENVIRONMENT}': no ${DEPLOY_CONFIG}" >&2
  exit 1
fi

STACK_NAME=$(python3 -c "import yaml; print(yaml.safe_load(open('${DEPLOY_CONFIG}'))['cloudformation_stack_name'])")
DAGS_BUCKET=$(python3 -c "import yaml; print(yaml.safe_load(open('${DEPLOY_CONFIG}'))['mwaa_dags_bucket'])")

echo "Deploying retail-aws-data-platform to ${ENVIRONMENT} (stack: ${STACK_NAME})"

aws cloudformation deploy \
  --template-file infrastructure/cloudformation/s3-data-lake.yaml \
  --stack-name "${STACK_NAME}" \
  --parameter-overrides "Environment=${ENVIRONMENT}" \
  --no-fail-on-empty-changeset

aws s3 sync dist/glue/jobs "s3://retail-${ENVIRONMENT}-deployment-artifacts/glue/jobs/"
aws s3 sync dist/emr/jobs "s3://retail-${ENVIRONMENT}-deployment-artifacts/emr/jobs/"
aws s3 sync dist/airflow/dags "s3://${DAGS_BUCKET}/dags/"
aws s3 sync dist/airflow/plugins "s3://${DAGS_BUCKET}/plugins/"

echo "Deployment to ${ENVIRONMENT} complete."
