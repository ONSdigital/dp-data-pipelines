#!/bin/bash -eux

source dp-data-pipelines/ci/scripts/shared.sh

# Copies entire project directory to the CI build location
copy_project() {
    log_info "Copying project"
    cp -r $SOURCE_DIR/* "$BUILD_DIR"
}

# Copies the Lambda handler Python script to a standardised location with a standardised name
# Ensures we can use the same Dockerfile for each Lambda with no changes (so far...)
copy_lambda_source() {
    local lambda_source=$(get_lambda_handler)
    local lambda_target="$SOURCE_DIR/lambda.py"

    log_info "Copying lambda source: $IMAGE_NAME"
    log_info "  From: $lambda_source"
    log_info "  To: $lambda_target"

    if [[ -f "$SOURCE_DIR/$lambda_source" ]]; then
        cp "$SOURCE_DIR/$lambda_source" "$lambda_target"
    else
        log_error "Lambda source file not found: $SOURCE_DIR/$lambda_source"
        exit 1
    fi
}

# Verify variables, copy lambda file, copy project
build_lambda() {
    log_info "Starting build script for lambda: $IMAGE_NAME"

    validate_environment
    validate_valid_lambda_name

    copy_lambda_source
    copy_project

    log_info "Build script completed successfully for: $IMAGE_NAME"
}

build_lambda
