# Pipelines

Documentation of the pipeline variants that can be called from this directory.

## dataset_ingress_v1

The dataset ingress v1 pipeline is intended for the use case of:

- given a path to a directory with sources files in it
- including a compatible ./pipeline-config.json (such as v1)
- create a csv and a json metadata file from source data (such as, but not restricted to an sdmx file).
- upload the csv to the [dp-upload-service](https://github.com/ONSdigital/dp-upload-service).
- upload any supplementary distributions (see [pipeline-config readme](../docs/pipeline-config.md) for details) to the [dp-upload-service](https://github.com/ONSdigital/dp-upload-service). Such as (but not restricted to) an sdmx file.
- upload the metadata to the [dp-dataset-api](https://github.com/ONSdigital/dp-dataset-api)
- inform the publishing support team that this new data has been uploaded.

It's designed to work with the v1 [pipeline-config.json](../docs/pipeline-config.md). The code is decoupled enough that later config iterations are possible without changing the pipeline utilising said config, though this is not planned at time of writing.s

The config for this pipeline provides:

- 1-n Source files
- 0-n Supplementary files to be uploaded alongside csv and metadata (such as an sdmx file)
- a "pipeline_details" key - this identifies the transformation code required to convert the SDMX to csv+metadata along with specifying what sanity checks to run. Please see [pipeline-details](../shared/details.py).

Beyond this high level view, the [python code that represents this](./dataset_ingress_v1.py) is a conciously simple set of linear commands and should be fairly easy to follow.