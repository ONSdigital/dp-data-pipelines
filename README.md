**WORK IN PROGRESS - DO NOT USE**

# dp-data-pipelines

Pipeline specific python scripts and tooling for automated website data ingress.

There are two things to understand here:

- Architecture
- Implementation

We'll take each in turn.


### Architecture

The following diagram shows a simple overview of how this repository is used to manage variants of simple data pipelines for the purpose of shunting data assets from outside the websites services to within - via submissions to an aws s3 bucket.

Note - This is a high level diagram only of _how_ we run pipeline**s** (as in, the usage of this github repo and how it interacts with infrastructure) to contextualise the code present rather than a strict architectural diagram.

_How_ the pipelines mentioned below actually work is covered in the following _Implementation_ section.

![Overview](./docs/overview.png)


### Implementation

This system works as follow:

- The lambda triggers a builspec, for an example lets use [pipeline-ingress.v1.yml](./pipeline-ingress-v1.yml)
- This particular buildspec triggers [./pipelines/ingress_v1.py: ingress_v1()](./pipelines/ingress_v1.py)
- The pipeline will have an s3 url which will be decompressed to the working dir, it provides:
     - source data
     - configuration options (such as which "transform" script to be applied to the source data)

For specific details using the above example please see:

- 1.) [The pipeline-config.json - v1](./docs/pipeline-config.md).
- 2.) [The ./pipelines readme - ingress_v1](./pipelines/README.md)
- 3.) [The buildspec pipeline-ingress.v1.yml](./pipeline-ingress-v1.yml)

