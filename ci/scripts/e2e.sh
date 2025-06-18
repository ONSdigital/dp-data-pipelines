#!/bin/bash -eux

# Run E2E tests (non-existing so far)

pushd dp-data-pipelines
    python ./tests/e2e/e2e_tests.py
popd