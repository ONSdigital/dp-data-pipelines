#!/bin/bash -eux

source dp-data-pipelines/ci/scripts/shared.sh

check_lambda_name

before_build_etl() {
  cp ./lambdas/lambda_runs_etl/Dockerfile ./Dockerfile.concourse
}

before_build_trigger() {
  cp ./lambdas/lambda_triggers_etl/Dockerfile ./Dockerfile.concourse
}

before_build_retry() {
  cp ./lambdas/lambda_retry_etl/Dockerfile ./Dockerfile.concourse
}

pushd dp-data-pipelines
  echo "before build step"
  handle_lambda_action "before_build"
popd