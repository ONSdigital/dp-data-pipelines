# dp-python-tools

Many of the modules in `dp-data-pipelines` rely on functionality from the [`dp-python-tools` repository](https://github.com/ONSdigital/dp-python-tools). There are `README` files for all available tools in the repository linked above, so detailed information is not included here, but brief descriptions of how these tools are used in `dp-data-pipelines` are given below.

## Database tools

### `DBCollection`

Provides methods for creating, reading and updating documents in an AWS DocumentDB collection.

### `DocumentDBClient`

Provides methods for managing AWS DocumentDB connections and retrieving DocumentDB collections.

## Email

### `SesClient`

Provides methods for creating an AWS SES client and sending emails.

## HTTP

As well as the classes described below, modules in the `dpytools.http` folder provide functionality for managing authentication tokens and implementing exponential backoff for HTTP requests.

### `DatasetAPIService`

Provides methods for uploading metadata to the [dp-dataset-api](https://github.com/ONSdigital/dp-dataset-api) and handling responses.

### `UploadServiceClient`

Provides methods for uploading data files to the [dp-upload-service](https://github.com/ONSdigital/dp-upload-service), including parameter generation and file chunking.

## Logging

### `DpLogger`

Provides methods for structured logs that conform to [DP logging standards](https://github.com/ONSdigital/dp-standards/blob/main/LOGGING_STANDARDS.md).

## Slack

### `SlackMessenger`

Provides methods for connecting to a Slack channel and sending messages.

## Stores

### `LocalDirectoryStore`

Provides methods for file operations on files in a local directory, including converting JSON files to Python dictionaries and finding files using regex matching.