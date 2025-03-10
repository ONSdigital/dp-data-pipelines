[[TOC]]

## Introduction

This document outlines the user flows for successful and failed pipeline operations, including the email content sent to the user (if any), the logging message that is output for a specific failure, and the full stack trace of the error. To run any scenario, unzip the test cases in `tests/data.zip` and run the code in the "Inputs" section.

All of the scenarios below assume that the data producer is submitting a data.csv file. Currently supported data file formats are CSV, XML, XLSX, CSDB. The stated error should be raised for any of these file types.

## User roles

### Data producer

TODO

## Email notifications

### Successful ingest

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json
3) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/valid/csv", pipeline_config=config)
```

#### Acceptance criteria

2 emails sent to `fileAuthorEmail` provided in manifest.json.
data.csv published via Upload Service.
Metadata available via Dataset API.

#### Email content

Email 1:
Subject:    File Upload: Completed Successfully
Message:	The file 'data.csv' has been uploaded successfully.

Email 2:
Subject:    Dataset Ingest: Metadata Submitted
Message: 	The metadata for {dataset_id} has been successfully submitted to the Dataset API

#### Logging message

N/a

#### Full stack trace

N/a

### Invalid data format

#### Scenario

Data producer submits 3 files:
1) data.txt (unsupported format)
2) metadata.json
3) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.txt$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/txt", pipeline_config=config)
```

#### Acceptance criteria

1 email sent to `fileAuthorEmail` provided in manifest.json

#### Email content

Subject:  ETL Pipeline error has occurred in Section: 1.1
Message:  An error has occurred in section: 1.1
			    Failed to upload file
			    Additional Data: {'file_path': PosixPath('data/txt/data.txt'), 'level': 'INFO'}

#### Logging message

```json
{
    "severity": 3,
    "created_at": "2025-02-21T12:58:46.273395+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"file_path": "PosixPath('data/txt/data.txt')", "level": "INFO"},
    "response_dict": "null",
    "raw": "null",
    "errors": "null",
    "event": "Error in section: 1.1 Failed to upload file",
    "timestamp": "2025-02-21T12:58:46.273444Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 163, in dataset_ingress_v1
    raise NotImplementedError(
NotImplementedError: Uploading file type .txt not currently supported.

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(files_dir="data/txt", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 180, in dataset_ingress_v1
    error_handler(
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/shared/error_handler_module.py", line 52, in error_handler
    raise Exception(error)
Exception: Failed to upload file
```

### Invalid metadata (`dataset_id`)

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json (invalid `dataset_id`)
3) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/invalid_metadata/dataset_id", pipeline_config=config)
```

#### Acceptance criteria

1 email sent to `fileAuthorEmail` provided in manifest.json

#### Email content

Subject:  ETL Pipeline error has occurred in Section: 1.1
Message:	An error has occurred in section: 1.1
			    Error getting Dataset API path for given dataset_path and edition_path
			    Additional Data: {'dataset_api_url': 'http://localhost:22000/datasets',
			    'dataset_path': 'invalid', 'edition_path': 'time-series', 'level': 'INFO'}"

#### Logging message

```json
{
    "severity": 3, 
     "created_at": "2025-02-27T14:23:08.271245+00:00", 
    "namespace": "data-ingress-pipelines", 
    "trace_id": "not-implemented", 
    "span_id": "not-implemented", 
    "data": {"level": "INFO"}, 
    "response_dict": null, 
    "raw": null, 
    "errors": null, 
    "event": "Error in section: 1.1 Metadata submission to Dataset API failed", 
    "timestamp": "2025-02-27T14:23:08.271308Z"
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 242, in dataset_ingress_v1
    dataset_api_get_path_response.raise_for_status()
  File "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/requests/models.py", line 1024, in raise_for_status
    raise HTTPError(http_error_msg, response=self)
requests.exceptions.HTTPError: 404 Client Error: Not Found for url: http://localhost:22000/datasets/invalid/editions/time-series/versions

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 52, in <module>
    dataset_ingress_v1(files_dir="data/invalid_metadata/dataset_id", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 244, in dataset_ingress_v1
    error_handler(
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/shared/error_handler_module.py", line 52, in error_handler
    raise Exception(error)
Exception: Metadata submission to Dataset API failed
```

### Invalid metadata (`edition_id`)

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json (invalid `edition_id`)
3) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/invalid_metadata/edition_id", pipeline_config=config)
```

#### Acceptance criteria

1 email sent to `fileAuthorEmail` provided in manifest.json

#### Email content

Subject:  ETL Pipeline error has occurred in Section: 1.1
Message:	An error has occurred in section: 1.1
			    Error getting Dataset API path for given dataset_path and edition_path
			    Additional Data: {'dataset_api_url': 'http://localhost:22000/datasets',
		    	'dataset_path': 'trade', 'edition_path': 'invalid', 'level': 'INFO'}"

#### Logging message

```json
{
  "severity": 3, 
  "created_at": "2025-02-27T14:28:08.587223+00:00", 
  "namespace": "data-ingress-pipelines", 
  "trace_id": "not-implemented", 
  "span_id": "not-implemented", 
  "data": {"level": "INFO"}, 
  "response_dict": null, 
  "raw": null, 
  "errors": null, 
  "event": "Error in section: 1.1 Metadata submission to Dataset API failed", 
  "timestamp": "2025-02-27T14:28:08.587287Z"
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 242, in dataset_ingress_v1
    dataset_api_get_path_response.raise_for_status()
  File "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/requests/models.py", line 1024, in raise_for_status
    raise HTTPError(http_error_msg, response=self)
requests.exceptions.HTTPError: 404 Client Error: Not Found for url: http://localhost:22000/datasets/trade/editions/invalid/versions

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 52, in <module>
    dataset_ingress_v1(files_dir="data/invalid_metadata/edition_id", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 244, in dataset_ingress_v1
    error_handler(
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/shared/error_handler_module.py", line 52, in error_handler
    raise Exception(error)
Exception: Metadata submission to Dataset API failed
```

### Invalid metadata (missing required field)

**Note - this is currently not working as expected due to the ongoing development of the Dataset API.**

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json (missing required field)
3) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/invalid_metadata/required_field", pipeline_config=config)
```

#### Acceptance criteria

1 email sent to `fileAuthorEmail` provided in manifest.json

#### Email content

Subject:  ETL Pipeline error has occurred in Section: 1.1
Message:	An error has occurred in section: 1.1
		  	Error getting Dataset API path for given dataset_path and edition_path
		  	Additional Data: {'dataset_api_url': 'http://localhost:22000/datasets',
		  	'dataset_path': 'trade', 'edition_path': 'time-series', 'level': 'INFO'}"

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T13:59:50.128975+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {
        "metadata": {"..."},
        "level": "ERROR",
    },
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "'dcterms:title'",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/utils.py",
                "function": "get_post_request_values_from_metadata",
                "line": 47,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Error getting POST request values from metadata",
    "timestamp": "2025-02-21T13:59:50.130847Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 213, in dataset_ingress_v1
    get_post_request_values_from_metadata(metadata)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/utils.py", line 84, in get_post_request_values_from_metadata
    raise err
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/utils.py", line 47, in get_post_request_values_from_metadata
    "title": metadata["dcterms:title"],
KeyError: 'dcterms:title'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 255, in dataset_ingress_v1
    "dataset_path": dataset_path,
UnboundLocalError: local variable 'dataset_path' referenced before assignment
```

## Validation errors - invalid JSON

### Invalid JSON (metadata.json)

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json (invalid JSON)
3) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/invalid_json/metadata", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T10:53:20.657808+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/invalid_json/metadata", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "File is not valid JSON: Expecting value: line 1 column 1 (char 0)",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py",
                "function": "validate_json_file",
                "line": 84,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T10:53:20.658756Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 81, in validate_json_file
    data = json.load(f)
  File "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/json/__init__.py", line 293, in load
    return loads(fp.read(),
  File "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/json/__init__.py", line 346, in loads
    return _default_decoder.decode(s)
  File "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/json/decoder.py", line 337, in decode
    obj, end = self.raw_decode(s, idx=_w(s, 0).end())
  File "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/json/decoder.py", line 355, in raw_decode
    raise JSONDecodeError("Expecting value", s, err.value) from None
json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1("data/invalid_json/metadata", config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 24, in validate_pipeline_files
    metadata_dict = validate_json_file(files_dir / "metadata.json")
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 84, in validate_json_file
    raise ValueError(f"File is not valid JSON: {str(e)}")
ValueError: File is not valid JSON: Expecting value: line 1 column 1 (char 0)
```

### Invalid JSON (manifest.json)

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json
3) manifest.json (invalid JSON)

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/invalid_json/manifest", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T10:56:31.977188+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/invalid_json/manifest", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "File is not valid JSON: Expecting value: line 1 column 1 (char 0)",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py",
                "function": "validate_json_file",
                "line": 84,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T10:56:31.978264Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 81, in validate_json_file
    data = json.load(f)
  File "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/json/__init__.py", line 293, in load
    return loads(fp.read(),
  File "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/json/__init__.py", line 346, in loads
    return _default_decoder.decode(s)
  File "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/json/decoder.py", line 337, in decode
    obj, end = self.raw_decode(s, idx=_w(s, 0).end())
  File "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/json/decoder.py", line 355, in raw_decode
    raise JSONDecodeError("Expecting value", s, err.value) from None
json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1("data/invalid_json/manifest", config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 22, in validate_pipeline_files
    manifest_dict = validate_json_file(files_dir / "manifest.json")
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 84, in validate_json_file
    raise ValueError(f"File is not valid JSON: {str(e)}")
ValueError: File is not valid JSON: Expecting value: line 1 column 1 (char 0)
```

## Validation errors - empty file

### Empty file (data.csv)

#### Scenario

Data producer submits 3 files:
1) data.csv (empty file)
2) metadata.json
3) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/empty/csv", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T11:03:08.014487+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/empty/csv", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "'data/empty/csv/data.csv' is empty",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_ingest_files.py",
                "function": "file_size_0",
                "line": 11,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T11:03:08.015200Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(files_dir="data/empty/csv", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 29, in validate_pipeline_files
    validate_pattern_files(files_dir, pipeline_config, "required_files")
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 62, in validate_pattern_files
    validate_file_exists_and_not_empty(file)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 73, in validate_file_exists_and_not_empty
    if file_size_0(file_path, give_error=True):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_ingest_files.py", line 11, in file_size_0
    raise ValueError(f"'{filepath}' is empty")
ValueError: 'data/empty/csv/data.csv' is empty
```

### Empty file (metadata.json)

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json (empty file)
3) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/empty/metadata", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T11:06:08.798884+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/empty/metadata", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "'data/empty/metadata/metadata.json' is empty",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_ingest_files.py",
                "function": "file_size_0",
                "line": 11,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T11:06:08.799584Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(files_dir="data/empty/metadata", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 19, in validate_pipeline_files
    validate_file_exists_and_not_empty(files_dir / file_name)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 73, in validate_file_exists_and_not_empty
    if file_size_0(file_path, give_error=True):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_ingest_files.py", line 11, in file_size_0
    raise ValueError(f"'{filepath}' is empty")
ValueError: 'data/empty/metadata/metadata.json' is empty
```

### Empty file (manifest.json)

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json
3) manifest.json (empty file)

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/empty/manifest", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T11:10:18.950730+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/empty/manifest", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "'data/empty/manifest/manifest.json' is empty",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_ingest_files.py",
                "function": "file_size_0",
                "line": 11,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T11:10:18.951567Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(files_dir="data/empty/manifest", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 19, in validate_pipeline_files
    validate_file_exists_and_not_empty(files_dir / file_name)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 73, in validate_file_exists_and_not_empty
    if file_size_0(file_path, give_error=True):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_ingest_files.py", line 11, in file_size_0
    raise ValueError(f"'{filepath}' is empty")
ValueError: 'data/empty/manifest/manifest.json' is empty
```

## Validation errors - missing file

### Missing file (data.csv)

#### Scenario

Data producer submits 2 files (data.csv missing):
1) metadata.json
2) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/missing/csv", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T11:14:48.439592+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/missing/csv", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "No files found matching pattern: ^data.csv$",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py",
                "function": "validate_pattern_files",
                "line": 59,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T11:14:48.440155Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(files_dir="data/missing/csv", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 29, in validate_pipeline_files
    validate_pattern_files(files_dir, pipeline_config, "required_files")
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 59, in validate_pattern_files
    raise FileNotFoundError(f"No files found matching pattern: {pattern}")
FileNotFoundError: No files found matching pattern: ^data.csv$
```

### Missing file (metadata.json)

#### Scenario

Data producer submits 2 files (metadata missing):
1) data.csv
2) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/missing/metadata", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T11:17:04.268256+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/missing/metadata", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "Required file not found: data/missing/metadata/metadata.json",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py",
                "function": "validate_file_exists_and_not_empty",
                "line": 71,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T11:17:04.269139Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(files_dir="data/missing/metadata", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 19, in validate_pipeline_files
    validate_file_exists_and_not_empty(files_dir / file_name)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 71, in validate_file_exists_and_not_empty
    raise FileNotFoundError(f"Required file not found: {file_path}")
FileNotFoundError: Required file not found: data/missing/metadata/metadata.json
```

### Missing file (manifest.json)

#### Scenario

Data producer submits 2 files (manifest missing):
1) data.csv
2) metadata.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/missing/manifest", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T11:20:17.472797+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/missing/manifest", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "Required file not found: data/missing/manifest/manifest.json",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py",
                "function": "validate_file_exists_and_not_empty",
                "line": 71,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T11:20:17.473426Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(files_dir="data/missing/manifest", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 19, in validate_pipeline_files
    validate_file_exists_and_not_empty(files_dir / file_name)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 71, in validate_file_exists_and_not_empty
    raise FileNotFoundError(f"Required file not found: {file_path}")
FileNotFoundError: Required file not found: data/missing/manifest/manifest.json
```

### Missing file (supplementary distribution)

#### Scenario

Data producer submits 3 files (supplementary distribution missing):
1) data.csv
2) metadata.json
3) manifest.json

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [{"matches": "^data.txt$"}],
}

dataset_ingress_v1(files_dir="data/missing/supp_dist", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T10:36:05.757693+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/missing/supp_dist", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "No files found matching pattern: ^data.txt$",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py",
                "function": "validate_pattern_files",
                "line": 59,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T10:36:05.759606Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1("data/missing/supp_dist", config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 33, in validate_pipeline_files
    supplementary_files = validate_pattern_files(
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 59, in validate_pattern_files
    raise FileNotFoundError(f"No files found matching pattern: {pattern}")
FileNotFoundError: No files found matching pattern: ^data.txt$
```

## Validation errors - other

### manifest.json missing required keys

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json
3) manifest.json (missing required keys)

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/missing/manifest_keys", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T12:34:28.751097+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/missing/manifest_keys", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "'Missing required keys in manifest: manifestVersion, source_id, fileAuthorEmail'",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py",
                "function": "validate_manifest_vars",
                "line": 92,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T12:34:28.751907Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(files_dir="data/missing/manifest_keys", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 23, in validate_pipeline_files
    validate_manifest_vars(manifest_dict, required_keys)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 92, in validate_manifest_vars
    raise KeyError(f"Missing required keys in manifest: {', '.join(missing_keys)}")
KeyError: 'Missing required keys in manifest: manifestVersion, source_id, fileAuthorEmail'
```


### manifest.json missing valid email

#### Scenario

Data producer submits 3 files:
1) data.csv
2) metadata.json
3) manifest.json (invalid email)

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/missing/invalid_email", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 1,
    "created_at": "2025-02-21T12:40:19.406276+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"files_dir": "data/missing/valid_email", "level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "Invalid email address: not a valid address. Error: An email address must have an @-sign.",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/shared/utils.py",
                "function": "get_submitter_email",
                "line": 61,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Pipeline validation failed",
    "timestamp": "2025-02-21T12:40:19.409804Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/messages/utils.py", line 59, in get_submitter_email
    validate_email(submitter_email)
  File "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/email_validator/validate_email.py", line 71, in validate_email
    = split_email(email)
  File "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/email_validator/syntax.py", line 123, in split_email
    left_part, right_part = split_string_at_unquoted_special(email, ("@", "<"))
  File "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/email_validator/syntax.py", line 86, in split_string_at_unquoted_special
    raise EmailSyntaxError("An email address must have an @-sign.")
email_validator.exceptions_types.EmailSyntaxError: An email address must have an @-sign.

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 50, in <module>
    dataset_ingress_v1(files_dir="data/missing/valid_email", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 39, in dataset_ingress_v1
    validation_results = validate_pipeline_files(files_dir, pipeline_config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 23, in validate_pipeline_files
    validate_manifest_vars(manifest_dict, required_keys)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/validate_pipeline.py", line 95, in validate_manifest_vars
    get_submitter_email(manifest_dict)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/messages/utils.py", line 61, in get_submitter_email
    raise ValueError(f"Invalid email address: {submitter_email}. Error: {str(e)}")
ValueError: Invalid email address: not a valid address. Error: An email address must have an @-sign
```

## Internal pipeline errors

### Upload Service URL env var not set

#### Scenario

UPLOAD_SERVICE_URL env var not set

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/valid/csv", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 3,
    "created_at": "2025-02-21T14:37:49.236314+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"level": "INFO"},
    "response_dict": "null",
    "raw": "null",
    "errors": "null",
    "event": "Error in section: 1.1 Failed to retrieve Upload Service/Dataset API URL",
    "timestamp": "2025-02-21T14:37:49.236335Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 109, in dataset_ingress_v1
    assert (
AssertionError: UPLOAD_SERVICE_URL environment variable not set

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 51, in <module>
    dataset_ingress_v1(files_dir="data/valid/csv", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 117, in dataset_ingress_v1
    error_handler(
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/shared/error_handler_module.py", line 52, in error_handler
    raise Exception(error)
Exception: Failed to retrieve Upload Service/Dataset API URL
```

### Dataset API URL env var not set

#### Scenario

DATASET_API_URL env var not set

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/valid/csv", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 3,
    "created_at": "2025-02-21T14:37:49.236314+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"level": "INFO"},
    "response_dict": "null",
    "raw": "null",
    "errors": "null",
    "event": "Error in section: 1.1 Failed to retrieve Upload Service/Dataset API URL",
    "timestamp": "2025-02-21T14:37:49.236335Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 113, in dataset_ingress_v1
    assert (
AssertionError: DATASET_API_URL environment variable is not set

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 51, in <module>
    dataset_ingress_v1(files_dir="data/valid/csv", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 117, in dataset_ingress_v1
    error_handler(
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/shared/error_handler_module.py", line 52, in error_handler
    raise Exception(error)
Exception: Failed to retrieve Upload Service/Dataset API URL
```

### SES Email Identity env var not set

#### Scenario

SES_EMAIL_IDENTITY env var not set

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/valid/csv", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 3,
    "created_at": "2025-02-21T14:51:46.682982+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"level": "INFO"},
    "response_dict": "null",
    "raw": "null",
    "errors": "null",
    "event": "Error in section: 1.1 Failed to create email client",
    "timestamp": "2025-02-21T14:51:46.683010Z",
}
{
    "severity": 1,
    "created_at": "2025-02-21T14:51:46.683066+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "'SES_EMAIL_IDENTITY'",
            "stack_trace": {
                "file": "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/os.py",
                "function": "__getitem__",
                "line": 679,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Failed to send error email notification",
    "timestamp": "2025-02-21T14:51:46.683869Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 84, in dataset_ingress_v1
    email_client = get_email_client()
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/shared/utils.py", line 35, in get_email_client
    ses_email_identity = os.environ["SES_EMAIL_IDENTITY"]
  File "/Users/sarahjohnson/.pyenv/versions/3.9.21/lib/python3.9/os.py", line 679, in __getitem__
    raise KeyError(key) from None
KeyError: 'SES_EMAIL_IDENTITY'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/behave_debug.py", line 53, in <module>
    dataset_ingress_v1(files_dir="data/valid/csv", pipeline_config=config)
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/dataset_ingress_v1.py", line 90, in dataset_ingress_v1
    error_handler(
  File "/Users/sarahjohnson/code/dp-data-pipelines/dpypelines/pipeline/shared/error_handler_module.py", line 52, in error_handler
    raise Exception(error)
Exception: Failed to create email client
```

### SES Email Identity env var not a valid email address

#### Scenario

SES_EMAIL_IDENTITY env var not a valid email address

#### Inputs

```python
config = {
    "config_version": 1,
    "transform": None,
    "transform_inputs": {},
    "transform_kwargs": {},
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}

dataset_ingress_v1(files_dir="data/valid/csv", pipeline_config=config)
```

#### Acceptance criteria

Error raised

#### Logging message

```json
{
    "severity": 3,
    "created_at": "2025-02-21T14:56:08.293763+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"level": "INFO"},
    "response_dict": "null",
    "raw": "null",
    "errors": "null",
    "event": "Error in section: 1.1 Failed to create email client",
    "timestamp": "2025-02-21T14:56:08.293832Z",
}
{
    "severity": 1,
    "created_at": "2025-02-21T14:56:08.293908+00:00",
    "namespace": "data-ingress-pipelines",
    "trace_id": "not-implemented",
    "span_id": "not-implemented",
    "data": {"level": "ERROR"},
    "response_dict": "null",
    "raw": "null",
    "errors": [
        {
            "message": "Invalid sender email: An email address must have an @-sign.",
            "stack_trace": {
                "file": "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/dpytools/email/ses/client.py",
                "function": "__init__",
                "line": 36,
            },
            "data": {
                "full": [
                    "Traceback (most recent call last):...",
                ]
            },
        }
    ],
    "event": "Failed to send error email notification",
    "timestamp": "2025-02-21T14:56:08.294987Z",
}
```

#### Full stack trace

```
Traceback (most recent call last):
  File "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/dpytools/email/ses/client.py", line 32, in __init__
    validated_email = validate_email(sender)
  File "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/email_validator/validate_email.py", line 71, in validate_email
    = split_email(email)
  File "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/email_validator/syntax.py", line 123, in split_email
    left_part, right_part = split_string_at_unquoted_special(email, ("@", "<"))
  File "/Users/sarahjohnson/code/dp-data-pipelines/.venv/lib/python3.9/site-packages/email_validator/syntax.py", line 86, in split_string_at_unquoted_special
    raise EmailSyntaxError("An email address must have an @-sign.")
email_validator.exceptions_types.EmailSyntaxError: An email address must have an @-sign.
```
