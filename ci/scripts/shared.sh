#!/bin/bash

check_lambda_name() {
  if [ -z "$LAMBDA_NAME" ]; then
    echo "Missing Lambda name variable"
    exit 1
  fi
}

handle_lambda_action() {
  local action_prefix=$1
  
  case "$LAMBDA_NAME" in
    "dp-sandbox-pipeline-etl")
      ${action_prefix}_etl
      ;;
    "dp-sandbox-pipeline-etl-trigger")
      ${action_prefix}_trigger
      ;;
    "dp-sandbox-pipeline-retry-etl")
      ${action_prefix}_retry
      ;;
    *)
      echo "Lambda $LAMBDA_NAME is not configured"
      ;;
  esac
}
