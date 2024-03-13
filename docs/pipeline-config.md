**NOTE - initial thoughts, not to be taken as gospel or indeed even sane/correct.**

**am putting some details in here beyond the config to put it in context, things will be more clearly seprated once this is finalised and documented for real. It's still quite early days on putting this all together.**

happy to take questions/convos - mike/martyn.

# pipeline-config.json

A "pipeline-config.json" is a small config file to be included in the tar file which consitutes a data submission from parties outisde of DP (and the website infrastructure) that we wish to ingress to appropriate services within said infrastructure..

The premise is this tar file appears in a bucket to trigger the event driven (bucket->lambda->glue) that runs the python scripts included in this repository.

The config is to provide basic details that are required to accomplish this.

## Ingress Config V1

The following is an initial sketch of a config. In our initial use case this is used for running:

- [s3_tar_recieved](https://github.com/ONSdigital/dp-data-pipelines/blob/sandbox/dpypelines/s3_tar_received.py)
- which calls [dataset_ingress_v1](https://github.com/ONSdigital/dp-data-pipelines/blob/sandbox/dpypelines/pipeline/dataset_ingress_v1.py)

This is decoupling, the arrival of a tar file we want to handle is not in of itself a guarantee its a dataset, we are keeping this distinction to allow to alternate use cases in the future.

_Note 1: these are umbrlla script for bringing together other classes and functions we're just finishing up writing, this "bringing together" is in flight at time of writing._

_Note 2: these things are schemas for a reason, nobody hits the bullseye first time and a different (or evolving) use case can necesitate a different config. Lets keep everything backwards compatibile._

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
    "pipeline": "dataset_ingress_v1",
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
| supplementary_distributions | regex patterns and counts for supplementary distributions that should be submitted alongside this config. For example: an sdmx file to be uploaded alongside the csv data derived from it. | Yes (but can be an empty list) | No |
| priority | Future proofing, currently unused | No | No |
| pipeline | The seconday pipeline to be called when we move from generic to specific handling | Yes | No |
| options -> pipeline_details | An identifier used to specify which data transformation function is ran against the source in question. Please see [../pipeline/shared/details.py](../pipeline/shared/details.py) | Yes | No |

The general idea here is that the "pipeline" remains the same, the changing parts are:

- what source files are recieved
- the transform function used to make them into the right shape.

The config is what provides this information. Along with other necessities (like who to email if the data is rubbish)

* The $id is provided as fully qualified url for ease of use but said schema is part of the codebase inside the pipeline and is just serving as a namespace, it is **never** called remotely as this would make the pipelines dependant on third party site (github) uptime.

**Important - I haven't document this yet for a reason, there are things still being worked out but I see tha value in starting a conversation about leftward requirements, just bear in mind we're a week or two of polish ahead of where we'd ideally want to be sharing this.**. For what it's worth I doubt it will be quite this, there's some messiness remaining.

This is also quite likely to actuall be `transform-pipeline-config.json` or similar , or the config schema should be, given it's linked to a principle use case, again, need to think on it.