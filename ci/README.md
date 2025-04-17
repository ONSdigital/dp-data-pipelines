# CI scripts

## before-build

[before-build.sh](./scripts/before-build.sh) runs before the start of a validation step in the CI pipeline, in job that builds the Docker image. Its purpose is to copy the Dockerfile for the relevant Lambda to the root of the repository, with the expected filename. This will ensure that it can be validated + amended as expected by the other step(s) in the Concourse CI workflow.

## build

[build.sh](./scripts/build.sh) is designed to copy the files/folders needed for the relevant Lambda's Docker image to the correct build folder for build. It runs immediately before the build step.