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

The tar file should include a pipeline-config.json. The configuration for the pipeline is defined in the configuration.py file. This JSON file contains the configuration details for the pipeline, such as the dataset id, the transform function to be used, the required files, and the function to be called after the main processing function (referred to as the "secondary function").

The CONFIGURATION dictionary in this file contains the configuration details for each dataset. The get_dataset_id() function is used to extract the dataset id from the S3 object name, and the get_pipeline_config() function is used to get the pipeline configuration details for the given dataset id.

For more information about the pipeline-config.json file, refer to the schema [here](dpypelines/schemas/dataset-ingress/config/v1.json).

```python
s3_tar_received.start()
```
The s3_tar_received.start() function performs the following steps:

1. Decompresses the tar file to the workspace.
2. Creates a local directory store using the decompressed files.
3. Extracts the dataset ID from the S3 object name.
4. Retrieves the configuration details for the given dataset ID from the pipeline-config.json file.
5. Calls the secondary function specified in the pipeline configuration. This function is defined in the dataset_ingress_v1.py file and is responsible for ingesting the data into the system.

Licence
-------

Copyright ©‎ 2024, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE) for details.