# pipeline-config.json


## Ingress Config V1

The following is an example of the dataset ingress v1 `pipeline-config.json`.

Note: This  is _intended_ for use with an dataset_ingress_v1 pipeline, but you could implement a new pipeline that can be configured by a config of this structure and equally you could create a v2 ingress config for use by the v1 ingress pipeline.

Simply put - pipelines and pipeline-configs are both versioned resource but while they are related most direct assumptions have been abstracted to the point where they only loosely coupled.

```json
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/schemas/dataset-ingress/config/v1.json",
    "required_files": [
        {
            "matches": "*.sdmx",
            "count": "1"
        }
    ],
    "supplementary_distributions": [
        {
            "matches": "*.sdmx",
            "count": "1"
        }
    ],
    "priority": "1",
    "pipeline": "pipeline-dataset-ingress-v1.yml",
    "options": {
        "pipeline_details": "sdmx.default" 
    }
}
```

explanations of fields follows:

| field | explanation | required | assumed in all config variations |
| ----- | ----------- | -------- | -------------------------------- |
| $schema | The version of the jsons schma standard in use. |  Yes | Yes |
| $id | a **uri*** uniquely identifying the schema for this config. | Yes | Yes |
| required_files | regex patterns and counts for source files that should be submitted alongside this config | Yes | No |
| supplementary_distributions | regex patterns and counts for supplementary distributions that should be submitted alongside this config | Yes (but can be an empty list) | No |
| priority | Future proofing, currently unused | No | No |
| pipeline | The buildspec to use when this pipeline is triggered | Yes | No |
| options -> pipeline_details | An identifier used to specifiy which data transformation function is ran against the source in question. Please see [../pipeline/shared/details.py](../pipeline/shared/details.py) | Yes | No |


* The $id is provided as fully qualified url for ease of use but said schema is part of the codebase inside the pipeline and is **never** called remotely as this would make the pipelines dependant on third party site uptime.