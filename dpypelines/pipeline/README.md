# Pipelines

Instructions on running existing pipelines can be found below. As more pipeline variants are added, this page will be updated.

## dataset_ingress_v1

### Default configuration

The `dataset_ingress_v1` pipeline function can be run directly with a local directory of files using the default configuration details.

```python
from dpypelines.pipeline.configuration import get_pipeline_config
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1

# `files_dir` should be a string of the directory containing a data.xml file
files_dir = "path/to/data/file"

# Get the default configuration details
default_pipeline_config, _ = get_pipeline_config("default")

dataset_ingress_v1(files_dir, default_pipeline_config)
```

Running `dataset_ingress_v1` will generate three files:
- a copy of the source data.xml file.
- a CSV file of the source data.
- a JSON file of the metadata associated with the CSV file.

If the `dataset_ingress_v1` function encounters any problems, an error will be raised that includes some information on the issue encountered, to help you resolve the problem. For example, if an invalid directory is passed as the `files_dir` argument, the following error will be raised:

```
Failed to access local data at <invalid_directory>

Error type: AssertionError
Error: Given path <invalid_directory> does not exist.
```