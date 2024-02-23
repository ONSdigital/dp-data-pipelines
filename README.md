**WORK IN PROGRESS - DO NOT USE**

# dp-data-pipelines

Pipeline specific python scripts and tooling for automated website data ingress.

There are two things to understand here:

- Architecture
- Implementation

We'll take each in turn then discuss repository structure.


### Architecture

The following diagram shows a simple overview of how this repository is used to manage variants of simple data pipelines for the purpose of shunting data assets from outside the websites services to within - via submissions to an aws s3 bucket.

Note - This is a high level diagram only of _how_ we run pipeline**s** (as in, the usage of this github repo and how it interacts with infrastructure) to contextualise the code present rather than a strict architectural diagram.

_How_ the pipelines mentioned below actually work is covered in the following _Implementation_ section.

![Overview](./docs/overview.png)


### Implementation

The initial example of this system works as follows:

- The lambda triggers the buildspec [./s3_tar_received.yml](./s3_tar_received.yml).
- This triggers [./pipelines/s3_tar_received.py](./pipelines/s3_tar_received.py) - this function triggers a pipeline from `./pipelines/pipeline` based on the [pipeline-config.json](./docs/pipeline-config.md) present in the tar file.
- **In our initial use case** it triggers our intial pipeline [dataset_ingress_v1.py](./pipelines/pipeline/dataset_ingress_v1.py).

The principle here is that the the details in pipeline config can be used to nuance which pipeline from `./pipelines/pipeline` is called as well as the options that are passed to it.

## Repository Structure

The following is the (initially, it'll expand over time) structure of this repository, **this does not need to be kept up to date with every change** - it's intended to help explain how the triggering logic for this repo works.

```
- /dp-data-pipelines
     - /schemas
     - /builder                           # Contains Dockerfile for image
     - /pipelines
          - s3_tar_received.py            # Uses contents of tar to decide wbich /pipeline/* to call
          - pipeline
               - /shared
               - dataset_ingress_v1.py
     - /tests
     - /features
     - s3_tar_received.yml                # Calls ./pipelines/s3_tar_recieved.py. Triggered by lambda
     - builder.yml                        # Rebuilds /builder/Dockerfile on changes to that directory
```
