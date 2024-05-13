# Data transit requirements

## Introduction

The recommendations below are predicated on the assumption that files should be uploaded to the DP Upload Service `/upload-new` endpoint. Files are uploaded to an S3 bucket that is already defined (for the Staging environment, this is the "ons-dp-sandbox-encrypted-datasets" bucket). Files larger than 5MB are chunked, and these chunks are uploaded separately and reassembled in the "datasets" folder of the "ons-dp-sandbox-encrypted-datasets" bucket.

## Reference documents

The sources below have been used in developing our [recommendations](#recommendations-for-manifestjson-structure):

[Transfer of input files](https://confluence.ons.gov.uk/display/DIS/Transfer+of+input+files) (Confluence)

[Transform Code Documentation (SDMX input)](https://confluence.ons.gov.uk/pages/viewpage.action?pageId=190307047)

[FileMetadata struct](https://github.com/ONSdigital/dp-api-clients-go/blob/main/files/data.go#L3)

[Resumable struct](https://github.com/ONSdigital/dp-upload-service/blob/develop/upload/upload.go#L19)

[dpytools UploadServiceClient._generate_new_upload_params()](https://github.com/ONSdigital/dp-python-tools/blob/develop/dpytools/http/upload.py#L238)

## Required fields for Transport Pipeline #3

The table below is copied from the [Transform pipeline #3](https://confluence.ons.gov.uk/display/DIS/Transfer+of+input+files#Transferofinputfiles-Transportpipeline#3) section. The fields that are currently being populated are referenced in the [`FileMetadata` struct](https://github.com/ONSdigital/dp-api-clients-go/blob/main/files/data.go#L3).

| Field              | Required? | Description                                                                                                                      | Current value                                                                                                           |
|--------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| fileAuthorEmail    | Mandatory | Email address of the file author. Used for notifications generated during data transformation and validation pipeline processing | Not currently populated                                                                                                 |
| fileAuthorUsername | Mandatory | Username of the file author. Used for notifications generated during data transformation and validation pipeline processing      | Not currently populated                                                                                                 |
| fileVersion        | Mandatory | Version number of the file. There may be a requirement to upload updated files of the same name                                  | Not currently populated                                                                                                 |
| isPublishable      | Mandatory | Whether the file is intended to by published to web and api users by the static file system                                      | `_generate_upload_new_params` (defaults to False)                                                                       |
| licence            | Mandatory | The licence that applies to the file once published to web and api users                                                         | `_generate_upload_new_params` (defaults to "Open Government Licence v3.0")                                              |
| licenceUrl         | Mandatory | The url where the licence described by the licence field is located                                                              | `_generate_upload_new_params` (defaults to "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/") |
| manifestVersion    | Mandatory | A version number to allow the support different manifest fields in the future, if required                                       | Not currently populated                                                                                                 |
| path               | Mandatory | The file path that the file should be available at once published to web and api users                                           | `_generate_upload_new_params` ("datasets/<timestamp>-<filename>")                                                       |
| aliasName          | Optional  | Alias for the file to be uploaded to the static file system                                                                      | `_generate_upload_new_params` (defaults to filename with extension if not provided)                                     |
| collectionId       | Optional  | Id of an existing collection in Florence that the file should be attached to once it is uploaded to the static file system       | Not currently populated                                                                                                 |

## Missing fields currently populated in HTTP params 

Most fields that are required for the [`Resumable` struct](https://github.com/ONSdigital/dp-upload-service/blob/develop/upload/upload.go#L19) (for uploading file chunks to the Upload Service) are missing from the table above, although there is some overlap, e.g. `aliasName`. Need to clarify what should be passed for `resumableRelativePath` (currently passing the local path of the file to be uploaded).

The following fields are defined in the [`FileMetadata` struct](https://github.com/ONSdigital/dp-api-clients-go/blob/main/files/data.go#L3), but there is no reference to these in the [Transform pipeline #3](https://confluence.ons.gov.uk/display/DIS/Transfer+of+input+files#Transferofinputfiles-Transportpipeline#3) table.

| Field       | Current                                                                                                  |
|-------------|----------------------------------------------------------------------------------------------------------|
| Title       | `_generate_upload_new_params` (defaults to filename without extension if not provided)                   |
| SizeInBytes | `_generate_upload_new_params` (total file size)                                                          |
| Type        | `_generate_upload_new_params` (mimetype of file to be uploaded - currently supporting CSV and XML files) |
| State       | Not currently populated                                                                                  |
| Etag        | Not currently populated                                                                                  |

## Recommendations for `manifest.json` structure

```json
{

}
```