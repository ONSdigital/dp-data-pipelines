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
This package provides multiple data pipelines. The initial use case is the s3_tar_received.start() function, which handles the required behavior when receiving a tar file indicated by an S3 object name.

Here's a basic example of how to use it:

```python
from dpypelines import s3_tar_received

s3_tar_received.start('my-bucket/my-data.tar')
```
In this example, 'my-bucket/my-data.tar' is the S3 object name of the tar file to be processed.

The CONFIGURATION dictionary in this file contains the configuration details for each dataset and the function to be called after the main processing function (referred to as the "secondary function"). The get_dataset_id() function is used to extract the dataset id, and the get_pipeline_config() function is used to get the pipeline configuration details for the given dataset id.

```python
s3_tar_received.start()
```
The s3_tar_received.start() function performs the following steps:

1. Decompresses the tar file to the workspace.
2. Creates a local directory store using the decompressed files.
3. Retrieves the configuration details for the given dataset from the configuration.py.
4. Calls the secondary function specified in the pipeline configuration. The secondary function defines which version of the transform functionality will be applied to the dataset.

If the s3_tar_received.start() function encounters any problems, an error will be raised that includes some information on the issue encountered, to help you resolve the problem. For example:

-   If it is not possible to create the local store from the decompressed tar file, an error message will be displayed indicating the issue with creating the local directory store.

Licence
-------

Copyright ©‎ 2024, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE) for details.