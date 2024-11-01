# `dataset_ingress_v1` function

The `dataset_ingress_v1()` function allows users to submit datasets for transformation, and optionally upload outputs to Digital Publishing systems.

## Running locally

The `dataset_ingress_v1` function can be run directly via a Python script as follows:

`myscript.py`
```python
from dpypelines.pipeline.configuration import get_pipeline_config
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1

# `files_dir` should be a string of the directory containing your data source(s)
files_dir = "path/to/directory/of/inputs"

# Specify the source id for accessing pipeline configuration details
pipeline_config = get_pipeline_config("<source_id>")

# Run the function
dataset_ingress_v1(files_dir, pipeline_config)
```

To run this script, open your terminal and enter the following command:

```bash
poetry run python3 ./myscript.py
```

**Note:** the `source_id` should match at least one key in the [`CONFIGURATION` dictionary](https://github.com/ONSdigital/dp-data-pipelines/blob/sandbox/dpypelines/pipeline/configuration.py#L11), otherwise an error will be raised.

**Before running this script, you will need to set a number of environment variables based on one of the following scenarios:**

### Scenario 1: Run the transform and view outputs without uploading to Digital Publishing

This is the scenario you will use most of the time when developing locally.

Set the required environment variables by opening your terminal and entering the following commands:

```bash
export DISABLE_NOTIFICATIONS=true
export SKIP_DATA_UPLOAD=true
export UPLOAD_SERVICE_URL=not-used
```

Then run `myscript.py` as [described above](#running-locally). This will run the full transform but will stop short of uploading outputs to the [dp-upload-service](https://github.com/ONSdigital/dp-upload-service).

The files created locally by the pipeline will be:

- A CSV file of the source data.
- A JSON file of the metadata associated with the CSV file.
- Each file listed under `supplementary_distributions` of the pipeline configuration details used.

### Scenario 2: Run the pipeline and upload outputs to Digital Publishing

This is the scenario to use if:
(a) you want to test the full pipeline functionality;
(b) you want to manually load and transform a file from your local machine and upload the results to the encrypted output S3 bucket, skipping the submission S3 bucket and AWS Glue job entirely.

**Note:** In order to upload outputs to the S3 bucket, you will need to have access to relevant Digital Publishing systems, and be able to generate a valid service token. Contact your Tech Lead in the first instance to arrange this.

Set the required environment variables by opening your terminal and entering the following commands:

```bash
export DISABLE_NOTIFICATIONS=true
export SKIP_DATA_UPLOAD=false
export UPLOAD_SERVICE_URL=<upload_service_url>
export SERVICE_TOKEN_FOR_UPLOAD=<service_token_for_upload>
```

Then run `myscript.py` as [described above](#running-locally). This will run the full transform and upload outputs to the [dp-upload-service](https://github.com/ONSdigital/dp-upload-service).

The files created by the pipeline and uploaded to the DP Upload Service will be:

- A CSV file of the source data.
- A JSON file of the metadata associated with the CSV file.
- Each file listed under `supplementary_distributions` of the pipeline configuration details used.
