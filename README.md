# dp-data-pipelines

Pipeline specific python scripts and tooling for automated website data ingress.

## Installation
This repository is provided as an installable python package

1. **Install the package**

    Open your terminal and run the following command:

    ```bash
    pip install git+https://github.com/ONSdigital/dp-data-pipelines.git
    ```

## Setup

Before you start using the `dp-data-pipelines` , you need to set up your environment:

1. **AWS SSO Login**: Ensure you are logged in to AWS via SSO. This is necessary for the function to access the S3 bucket and process the tar file. If you haven't logged in, you can do so by running `aws sso login` in your terminal.

## Usage
This package provides multiple data pipelines. The initial use case is the s3_tar_received.start() function, which handles the required behavior when receiving a tar file indicated by an S3 object name.

Here's a basic example of how to use it:

```python
from dpypelines import s3_tar_received

s3_tar_received.start('my-bucket/my-data.tar')
```
In this example, `my-bucket/my-data.tar` is the S3 object name of the tar file to be processed.

The `CONFIGURATION` dictionary in this file contains the configuration details for each dataset and the function to be called after the main processing function (referred to as the "secondary function"). The `get_dataset_id()` function is used to extract the dataset id, and the `get_pipeline_config()` function is used to get the pipeline configuration details for the given dataset id.

The `s3_tar_received.start()` function performs the following steps:

1. Decompresses the tar file to the workspace.
2. Creates a local directory store using the decompressed files.
3. Retrieves the configuration details for the given dataset from the `configuration.py`.
4. Calls the secondary function specified in the pipeline configuration. The secondary function defines which version of the transform functionality will be applied to the dataset.

If the `s3_tar_received.start()` function encounters any problems, an error will be raised that includes some information on the issue encountered, to help you resolve the problem. For example:

-   If it is not possible to create the local store from the decompressed tar file, an error message will be displayed indicating the issue with creating the local directory store.


## Configuration

| Environment variable     | Default | Description                                                     |
|:-------------------------|:-------:|:----------------------------------------------------------------|
| DE_SLACK_WEBHOOK         |  None   | Webhook for the Data Engineering Slack channel                  |
| DISABLE_NOTIFICATIONS    |  False  | Toggle Slack notifications on or off                            |
| AWS_PROFILE              |  None   | The AWS environment                                             |
| UPLOAD_SERVICE_URL       |  None   | The URL of the Upload Service                                   |
| UPLOAD_SERVICE_S3_BUCKET |  None   | The S3 bucket name to upload to                                 |
| FLORENCE_TOKEN           |  None   | The Florence access token required to access the Upload Service |


Licence
-------

Copyright ©‎ 2024, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE) for details.