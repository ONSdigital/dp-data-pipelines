# `s3_tar_received` pipeline

The `s3_tar_received` pipeline handles datasets that are received as a `.tar` file. The pipeline is triggered upon receipt of the `.tar` file into an AWS S3 bucket, which calls the `s3_tar_received.start()` function, with the S3 object name of the `.tar` file as an argument. The files contained within `my-bucket/my-data.tar` can be any format. The onwards pipeline is then configured by a combination of file extensions and a `manifest.json` file which **must** be included in `my-data.tar`.


## `s3_tar_received.start()` function

In order to run the `s2_tar_received.start()` function, you will need to set an environment variable indicating which AWS environment the S3 bucket is located in. To do this, open your terminal and enter the following command:

```bash
export AWS_PROFILE=<aws_profile_value>
```

In the example below, `my-bucket/my-data.tar` is the S3 object name of the `.tar` file to be processed:

`myscript.py`
```python
from dpypelines import s3_tar_received

s3_tar_received.start('my-bucket/my-data.tar')
```

To run this script, open your terminal and enter the following command:

```bash
poetry run python3 ./myscript.py
```

The `s3_tar_received.start()` function performs the following steps:

1. Decompresses the `my-data.tar` file to the workspace.
2. Creates a local directory store using the decompressed files.
3. Retrieves pipeline configuration details for the given dataset using the `source_id` field in `manifest.json`.
4. Calls the secondary function specified in the pipeline configuration details. This secondary function defines which transform functionality should be applied to the dataset.

## `manifest.json` file

The `.tar` file submitted to the pipeline **must** contain a file named `manifest.json`, which contains configuration details required for successful pipeline processing of submissions. Details of required fields are in the table below:

| Field                | Required? | Description                                                                                                                                                                                          | Default value                                                               |
|----------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `manifestVersion`    | Mandatory | A version number to support different manifest versions in the future, if required. Used to validate the submitted `manifest.json` file against a JSON schema to ensure required fields are present. | None                                                                        |
| `source_id`          | Mandatory | Used to get pipeline configuration details.                                                                                                                                                          | None                                                                        |
| `fileAuthorEmail`    | Mandatory | Email address of the file author. Used for notifications generated during pipeline processing.                                                                                                       | None                                                                        |
| `fileAuthorUsername` | Mandatory | Username of the file author. Used for notifications generated during pipeline processing.                                                                                                            | None                                                                        |
| `isPublishable`      | Optional  | Whether the file is intended to by published to web and API users by the static file system.                                                                                                         | False                                                                       |
| `licence`            | Optional  | The licence that applies to the file once published to web and API users.                                                                                                                            | "Open Government Licence v3.0"                                              |
| `licenceUrl`         | Optional  | The URL where the licence described by the `licence` field is located.                                                                                                                               | "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/" |
| `title`              | Optional  | The title of the file.                                                                                                                                                                               | Filename without extension                                                  |
| `aliasName`          | Optional  | Alias for the file to be uploaded to the static file system.                                                                                                                                         | Filename with extension                                                     |

## Pipeline configuration details

The `source_id` field in `manifest.json` is used to get pipeline configuration details, such as the relevant pipeline and transform functions, and lists of required files and supplementary distributions. These configuration details are stored in a Python dictionary which is described in the table below:

| Key                           | Value type and description                                                                                                                                                                                   |
|:------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `config_version`              | Integer value which supports different pipeline behaviour depending on which version of the pipeline configuration is being used.                                                                            |
| `transform`                   | The transformation function that should be applied to the dataset.                                                                                                                                           |
| `transform_inputs`            | A dictionary where the key is a regex pattern matching one of the files in the submission, and the value is a function that should be applied to the file, such as a validation or sanity checking function. |
| `transform_kwargs`            | A dictionary that supports the propagation of keyword arguments throughout the pipeline, where the key is the keyword argument name, and the value is the keyword argument value to be propagated.           |
| `required_files`              | A list of regex patterns matching required files that form part of the submission.                                                                                                                           |
| `supplementary_distributions` | A list of regex patterns matching supplementary distributions that form part of the submission.                                                                                                              |
| `secondary_function`          | The pipeline function that should be applied to the dataset.                                                                                                                                                 |

## Error handling

If the `s3_tar_received.start()` function encounters any problems, an error will be raised that includes information on the issue encountered, to help you resolve the problem. For example, if you attempt to call the `s3_tar_received.start()` function on a file without the `.tar` extension - in this case, `my-bucket/data.csv` - the following error will be raised:

```bash
    NotImplementedError: This function currently only handles archives using the tar extension. Got "my-bucket/data.csv"
```

## Secondary functions

The final step of the `s3_tar_received.start()` function calls the `secondary_function` specified in the pipeline configuration details. For more information on functions available at this step, please click on the links below:

- [`dataset_ingress_v1`](./pipeline/dataset_ingress_v1.md)
- [`generic_data_ingress_v1`](./pipeline/generic_file_ingress_v1.md)

