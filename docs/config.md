# Configuration

## ETL Lambda

Configuration for the ETL Lambda is managed in the [job_config.py](dpypelines/pipeline/config/job_config.py) file.

It uses [pydantic_settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) to:

1. Read secret values from AWS
2. Read values from a `.env` file
3. Read values from the OS environment

The settings are loaded in that order, and are overwritten if a matching key is supplied in a later load. E.g. if `EXAMPLE_VARIABLE` is set from AWS Secrets Manager, but `EXAMPLE_VARIABLE` is also set in the `.env` file, then the `.env` variable will take highest priority.