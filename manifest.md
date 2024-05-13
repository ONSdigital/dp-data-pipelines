# Data transit requirements

## Introduction

The recommendations below are predicated on the assumption that files should be uploaded to the DP Upload Service `/upload-new` endpoint. Files are uploaded to an S3 bucket that is already defined (for the Staging environment, this is the "ons-dp-sandbox-encrypted-datasets" bucket). Files larger than 5MB are chunked, and these chunks are uploaded separately and reassembled in the "datasets" folder of the "ons-dp-sandbox-encrypted-datasets" bucket.

## Reference documents

The sources below have been used in developing our [recommendations](#recommendations-for-manifestjson-structure):

[DIS706 Jira ticket](https://jira.ons.gov.uk/browse/DIS-706)

[Transfer of input files](https://confluence.ons.gov.uk/display/DIS/Transfer+of+input+files) (Confluence)

[Transform Code Documentation (SDMX input)](https://confluence.ons.gov.uk/pages/viewpage.action?pageId=190307047)

[FileMetadata struct](https://github.com/ONSdigital/dp-api-clients-go/blob/main/files/data.go#L3)

[Resumable struct](https://github.com/ONSdigital/dp-upload-service/blob/develop/upload/upload.go#L19)

[dpytools UploadServiceClient._generate_new_upload_params()](https://github.com/ONSdigital/dp-python-tools/blob/develop/dpytools/http/upload.py#L238)

## Required fields for Transport Pipeline #3

The table below is copied from the [Transform pipeline #3](https://confluence.ons.gov.uk/display/DIS/Transfer+of+input+files#Transferofinputfiles-Transportpipeline#3) section. The fields that are currently being populated are referenced in the [`FileMetadata` struct](https://github.com/ONSdigital/dp-api-clients-go/blob/main/files/data.go#L3) and [Resumable struct](https://github.com/ONSdigital/dp-upload-service/blob/develop/upload/upload.go#L19) definitions.

| Field              | Required? | Description                                                                                                                      | Current value                                                                                                           | JSON alias                               |
|--------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|------------------------------------------|
| fileAuthorEmail    | Mandatory | Email address of the file author. Used for notifications generated during data transformation and validation pipeline processing | Not currently populated                                                                                                 | Not in `Resumable` or `FileMetadata`     |
| fileAuthorUsername | Mandatory | Username of the file author. Used for notifications generated during data transformation and validation pipeline processing      | Not currently populated                                                                                                 | Not in `Resumable` or `FileMetadata`     |
| fileName           | Mandatory | Name of SDMX V2.0 to be transported to the data transformation and validation pipeline, and onwards to the static file system    | `_generate_upload_new_params` (generated from local path of file to be uploaded)                                        | `resumableFilename` (`Resumable` struct) |
| fileVersion        | Mandatory | Version number of the file. There may be a requirement to upload updated files of the same name                                  | Not currently populated                                                                                                 | Not in `Resumable` or `FileMetadata`     |
| isPublishable      | Mandatory | Whether the file is intended to by published to web and api users by the static file system                                      | `_generate_upload_new_params` (defaults to False)                                                                       | `is_publishable` (`FileMetadata` struct) |
| licence            | Mandatory | The licence that applies to the file once published to web and api users                                                         | `_generate_upload_new_params` (defaults to "Open Government Licence v3.0")                                              | `licence` (`FileMetadata` struct)        |
| licenceUrl         | Mandatory | The url where the licence described by the licence field is located                                                              | `_generate_upload_new_params` (defaults to "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/") | `licence_url` (`FileMetadata` struct)    |
| manifestVersion    | Mandatory | A version number to allow the support different manifest fields in the future, if required                                       | Not currently populated                                                                                                 | Not in `Resumable` or `FileMetadata`     |
| path               | Mandatory | The file path that the file should be available at once published to web and api users                                           | `_generate_upload_new_params` ("datasets/<timestamp>-<filename>")                                                       | `path` (`FileMetadata` struct)           |
| aliasName          | Optional  | Alias for the file to be uploaded to the static file system                                                                      | `_generate_upload_new_params` (defaults to filename with extension if not provided)                                     | `aliasName` (`Resumable` struct)         |
| collectionId       | Optional  | Id of an existing collection in Florence that the file should be attached to once it is uploaded to the static file system       | Not currently populated                                                                                                 | `collection_id` (`FileMetadata` struct)  |

## Missing fields currently populated in HTTP params 

Most fields that are required for the [`Resumable` struct](https://github.com/ONSdigital/dp-upload-service/blob/develop/upload/upload.go#L19) (for uploading file chunks to the Upload Service) are missing from the table above, although there is some overlap, e.g. `aliasName`. Need to clarify what should be passed for `resumableRelativePath` (currently passing the local path of the file to be uploaded).

The following fields are defined in the [`FileMetadata` struct](https://github.com/ONSdigital/dp-api-clients-go/blob/main/files/data.go#L3), but there is no reference to these in the [Transform pipeline #3](https://confluence.ons.gov.uk/display/DIS/Transfer+of+input+files#Transferofinputfiles-Transportpipeline#3) table.

| Field       | Current                                                                                                  | JSON alias      | Required? |
|-------------|----------------------------------------------------------------------------------------------------------|-----------------|-----------|
| Title       | `_generate_upload_new_params` (defaults to filename without extension if not provided)                   | `title`         | Yes       |
| SizeInBytes | `_generate_upload_new_params` (total file size?)                                                         | `size_in_bytes` | Yes       |
| Type        | `_generate_upload_new_params` (mimetype of file to be uploaded - currently supporting CSV and XML files) | `type`          | Yes       |
| State       | Not currently populated                                                                                  | `state`         | No        |
| Etag        | Not currently populated                                                                                  | `etag`          | No        |

## Pipeline requirements

Accept a `.tar` file containing the following files:
- `.xml` file to be transformed (only 1 file?)
- `manifest.json` file containing the metadata required to process and upload the `.xml` file.
- any additional files (e.g. supplementary distributions) that form part of the submission (no processing needed?).

Upload the following files to the relevant API:
- `.csv` file of data from the `.xml` input file generated by the pipeline transform (DP Upload Service API).
- `.json` file of catalog metadata associated with the `.csv` file generated by the pipeline transform (DP Dataset API?).
- All additional files contained in the original `.tar` submission (DP Upload Service API?).

## Additional fields required for pipeline transforms

- dataset id (for getting pipeline config details)
- email of data submitter (== `fileAuthorEmail`?)

## Recommendations for `manifest.json` structure

```json
{
    "dataset_id": "string",
    "fileAuthorEmail": "string",
    "fileAuthorUsername": "string",
    "fileVersion": "integer",
    "isPublishable": "boolean",
    "licence": "string",
    "licenceUrl": "string",
    "manifestVersion": "integer",
    "aliasName": "string",
    "collectionId": "string",
    "state": "Optional string",
    "etag": "Optional string"
}
```

Clarification needed:
- Issues with file chunking need to be resolved. Currently, files that are larger than 5MB cannot be uploaded, as this is raising a `DuplicateFileError` (we think this is happening [here](https://github.com/ONSdigital/dp-api-clients-go/blob/a26491512a8336ad9c31b694c045d8e3a3ed0578/files/client.go#L160)), because the `path` value is the same for each chunk. If the `path` value should be different for each chunk, what should it be?
- Should `path` be provided in `manifest.json` or do we want to generate it from a concatenation of pre-defined bucket name, timestamp and filename (or similar)?
- Is `fileAuthorEmail` the same as `publisherEmail`?
- `fileVersion` - what is this used for? May need careful handling. What if two files are uploaded with the same version number? What if someone tries to upload v1 when v2 has already been uploaded? How will version numbers be tracked?
- What should `resumableRelativePath` be (currently passing the local path of the file to be uploaded)?
- What should `state` and `etag` be? Can these be inferred or automatically generated?
- Do we need a JSON schema to validate `manifest.json` against (using `manifestVersion`)?
- Should additional files (supplementary distributions) be uploaded to the same bucket (and folder) as the CSV file?
- Should `manifest.json` contain anything that would be considered to be catalog metadata, e.g. issued/modified/release dates, or should these all come from the XML file?

| Field        | Description                                                               |
|--------------|---------------------------------------------------------------------------|
| `dataset_id` | Needed to populate `pipeline_config` dictionary in `dataset_ingress_v1()` |