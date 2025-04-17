#!/bin/bash -eux

source dp-data-pipelines/ci/scripts/shared.sh

check_lambda_name

build_etl() {
  cp ./lambdas/lambda_runs_etl/lambda_job_etl.py ../build
  cp -r ./ ../build/dp-data-pipelines
}

build_trigger() {
  cp ./lambdas/lambda_triggers_etl/lambda_triggers_etl.py ../build
}

build_retry() {
  cp ./lambdas/lambda_retry_etl/lambda_retry_etl.py ../build
}

pushd dp-data-pipelines
  cp ./Dockerfile.concourse ../build/Dockerfile.concourse
  handle_lambda_action "build"
popd