# Lambdas

This folder contains the source code for 2 Python AWS Lambda Functions:

- [lambda_triggers_etl.py](./lambda_triggers_etl): A Lambda triggered by an S3:PutObject event, that then invokes the `lambda_job_etl.py` Lambda
- [lambda_job_etl.py](./lambda_runs_etl): A Lambda that process the object that triggers the first Lambda