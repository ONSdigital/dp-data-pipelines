# Pipelines

Instructions on running existing pipelines can be found below. As more pipeline variants are added, this page will be updated.

## dataset_ingress_v1

### Running locally

The `dataset_ingress_v1` pipeline function can be run directly via a script like the following:

`myscript.py`
```python
from dpypelines.pipeline.configuration import get_pipeline_config
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1

# `files_dir` should be a string of the directory containing your data source(s)
files_dir = "path/to/directory/of/inputs"

# Specify the dataset id for picking the configuration you want to use for
# your source.
pipeline_config  = get_pipeline_config("<dataset_id>")

# Run the function
dataset_ingress_v1(files_dir, pipeline_config)
```

Which you would run via:

```
poetry run python3 ./myscript.py
```

Note: the key you choose should match at least one key in the `CONFIGURATION` dict, see [this file](https://github.com/ONSdigital/dp-data-pipelines/blob/sandbox/dpypelines/pipeline/configuration.py).

**Before** running this, you'll need to set some environment variables based on one of two scenarios as follows:

### Scenario 1: You dont care about uploading the results of the pipeline (you just want to run the transform and see the outputs)

This is probably the setup you'll want to use the majority of the time when developing locally.

Set the following env vars:

```
export DISABLE_NOTIFICATIONS=true
export SKIP_DATA_UPLOAD=true
```

Note: this will run the full transform but will stop short of uploading things to the [dp-upload-service](https://github.com/ONSdigital/dp-upload-service) such that they get into an encrypted bucket on s3.

The files created by the pipeline locally should be:

- a CSV file of the source data.
- a JSON file of the metadata associated with the CSV file.
- each file listed under "supplementary_distributions" for the config you used.


### Scenario 2: You want to transform the files and upload all assets to the [dp-upload-service](https://github.com/ONSdigital/dp-upload-service) such that they get into an encrypted bucket on s3.

This is a rarer case where (a) you want to trial the full pieline functionality or (b) you want to fully manually load a file from your local machine all the way into the encrypted s3 bucket and skip the submission bucket and aws glue job entirely.

TODO - will be covered in a ticket to come.