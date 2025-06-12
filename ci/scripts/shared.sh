#!/bin/bash -eux

##########
# Config #
##########
readonly BUILD_DIR="./build"
readonly SOURCE_DIR="./dp-data-pipelines"

# Lambda names -> containing folders
declare -A LAMBDA_FOLDERS=(
    ["dp-pipeline-etl"]="lambdas/lambda_runs_etl/"
    ["dp-submissions-pipeline"]="lambdas/lambda_triggers_etl/"
    ["dp-pipeline-retry-etl"]="lambdas/lambda_retry_etl/"
)

# Get the contianing folder for the Lambda
get_lambda_folder() {
    local lambda_folder="${LAMBDA_FOLDERS[${IMAGE_NAME}]}"
    echo "${lambda_folder}"
}

# lambda names -> lambda file names
declare -A LAMBDA_HANDLERS=(
    ["dp-pipeline-etl"]="lambda_job_etl.py"
    ["dp-submissions-pipeline"]="lambda_triggers_etl.py"
    ["dp-pipeline-retry-etl"]="lambda_retry_etl.py"
)

# Get the path for the Lambda file for the $IMAGE_NAME var
get_lambda_handler() {
    local handler_folder=$(get_lambda_folder)
    local handler_file="${LAMBDA_HANDLERS[${IMAGE_NAME}]}"
    local lambda_handler="${handler_folder}${handler_file}"

    echo "${lambda_handler}"
}

get_lambda_dockerfile() {
    local handler_folder=$(get_lambda_folder)
    local lambda_dockerfile="${handler_folder}Dockerfile"

    echo "${lambda_dockerfile}"
}

##############
# Validation #
##############

validate_environment() {
    if [[ -z "${IMAGE_NAME:-}" ]]; then
        log_error "IMAGE_NAME environment variable is required"
        exit 1
    fi
}

validate_valid_lambda_name() {
    if [[ ! -v "LAMBDA_HANDLERS[$IMAGE_NAME]" ]]; then
        log_error "Lambda '$IMAGE_NAME' is not configured"
        log_info "Available lambdas: ${!LAMBDA_HANDLERS[*]}"
        exit 1
    fi
}

###########
# Logging #
###########

log_info() {
    echo "[INFO] $*" >&2
}

log_error() {
    echo "[ERROR] $*" >&2
}
