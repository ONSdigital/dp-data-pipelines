#!/bin/bash -eux

pushd dp-data-pipelines
    python ./tests/e2e/e2e_tests.py
popd