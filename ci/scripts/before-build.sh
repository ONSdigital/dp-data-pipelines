#!/bin/bash -eux

source dp-data-pipelines/ci/scripts/shared.sh

# Copies the Dockerfile for the Lambdas to the expected location for the Concourse CI Pipeline
copy_docker_file() {
    local dockerfile_source="lambdas/Dockerfile"
    local dockerfile_target="$SOURCE_DIR/Dockerfile.concourse"

    log_info "Copying Dockerfile for Lambda: $IMAGE_NAME"
    log_info "  From: $dockerfile_source"
    log_info "  To: $dockerfile_target"

    if [[ -f "$SOURCE_DIR/$dockerfile_source" ]]; then
        cp "$SOURCE_DIR/$dockerfile_source" "$dockerfile_target"
    else
        log_error "Lambda source file not found: $SOURCE_DIR/$dockerfile_source"
        exit 1
    fi
}

before_build() {
    log_info "Starting before-build script for lambda: $IMAGE_NAME"

    copy_docker_file

    log_info "Before-build completed successfully for: $IMAGE_NAME"
}

before_build
