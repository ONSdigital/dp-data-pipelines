# Data Transformation and Validation pipeline input requirements

This document outlines the [input requirements](#inputs) for files to be submitted to the Data Transformation and Validation (DTV) pipeline. All pipeline submissions **must** include a `manifest.json` file for configuring the pipeline. Details of the fields that should be included in `manifest.json` are [outlined below](#manifestjson-structure).

## Inputs

The DTV pipeline is triggered when a single `.tar` file is uploaded to the designated AWS S3 bucket. This `.tar` file should contain the file(s) to be transformed, any supplementary distribution files, and a `manifest.json` file for configuring the pipeline.

Business areas wishing to submit datasets to the DTV pipeline will add the relevant files to an MS Sharepoint app/folder. The primary issue to be resolved is how the pipeline will be triggered when multiple files to be processed in one submission are added to Sharepoint.

## `manifest.json` structure

```json
{
    "manifestVersion": "integer",
    "source_id": "string",
    "fileAuthorEmail": "string",
    "fileAuthorUsername": "string",
    "isPublishable": "Optional boolean",
    "licence": "Optional string",
    "licenceUrl": "Optional string",
    "title": "Optional string",
    "aliasName": "Optional string"
}
```

| Field                | Required? | Description                                                                                                                                                                                          | Default value                                                               |
|----------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `manifestVersion`    | Mandatory | A version number to support different manifest versions in the future, if required. Used to validate the submitted `manifest.json` file against a JSON schema to ensure required fields are present. | None                                                                        |
| `source_id`          | Mandatory | Used to get the correct transform details within the DTV pipeline.                                                                                                                                   | None                                                                        |
| `fileAuthorEmail`    | Mandatory | Email address of the file author. Used for notifications generated during DTV pipeline. processing                                                                                                   | None                                                                        |
| `fileAuthorUsername` | Mandatory | Username of the file author. Used for notifications generated during DTV pipeline. processing                                                                                                        | None                                                                        |
| `isPublishable`      | Optional  | Whether the file is intended to by published to web and api users by the static file system.                                                                                                         | False                                                                       |
| `licence`            | Optional  | The licence that applies to the file once published to web and api users.                                                                                                                            | "Open Government Licence v3.0"                                              |
| `licenceUrl`         | Optional  | The url where the licence described by the licence field is located.                                                                                                                                 | "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/" |
| `title`              | Optional  | The title of the file.                                                                                                                                                                               | Filename without extension                                                  |
| `aliasName`          | Optional  | Alias for the file to be uploaded to the static file system.                                                                                                                                         | Filename with extension                                                     |
