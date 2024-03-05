**WORK IN PROGRESS - DO NOT USE**

# dp-data-pipelines

Pipeline specific python scripts and tooling for automated website data ingress.

## Installation
This repository is provided as an installable python package

1. **Install the package**

    Open your terminal and run the following command:

    ```bash
    pip install git+https://github.com/ONSdigital/dp-data-pipelines.git
    ```

## Usage

TODO: Add usage instructions here.

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

Licence
-------

Copyright ©‎ 2024, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE) for details.