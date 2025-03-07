# `s3_folder_received` pipeline

The `s3_folder_received` pipeline handles datasets that are received in a directory. The pipeline is triggered upon receipt of the directory containing the files into an AWS S3 bucket, which calls the `s3_folder_received.start()` function, with the S3 object name of the folder as an argument. The onwards pipeline is then configured by a combination of file extensions and a `manifest.json` file which **must** be included in `folder-containing-data`.


## `s3_folder_received.start()` function

In order to run the `s3_folder_received.start()` function, you will need to set an environment variable indicating which AWS environment the S3 bucket is located in. To do this, open your terminal and enter the following command:

```bash
export AWS_PROFILE=<aws_profile_value>
```

In the example below, `my-bucket/folder-containing-data` is the S3 object name of the folder containing the `.csv` and two `.json` files:

`myscript.py`
```python
from dpypelines import s3_folder_received

s3_folder_received.start('my-bucket/folder-containing-data')
```

To run this script, open your terminal and enter the following command:

```bash
poetry run python3 ./myscript.py
```

The `s3_folder_received.start()` function performs the following steps:

1. Downloads the files contained in `folder-containing-data` into a local directory.
2. Retrieves pipeline configuration details for the given dataset using the `source_id` field in `manifest.json`.
3. Calls the secondary function specified in the pipeline configuration details. This secondary function defines which transform functionality should be applied to the dataset.

## `manifest.json` file

The `folder-containing-data` file submitted to the pipeline **must** contain a file named `manifest.json`, which contains configuration details required for successful pipeline processing of submissions. 
Details of required fields for a `manifest.json` file can be found in the [Requirements](/requirements.md#fields) page:


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
## Secondary functions

The final step of the `s3_folder_received.start()` function calls the `secondary_function` specified in the pipeline configuration details. For more information on functions available at this step, please click on the links below:

- [`dataset_ingress_v1`](./pipeline/dataset_ingress_v1.md)

