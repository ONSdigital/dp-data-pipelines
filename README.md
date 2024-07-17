# dp-data-pipelines

Python scripts and tooling for automated website data ingress pipelines.

## Installation

This repository is provided as an installable Python package. To install it, open your terminal and enter the following command:

```bash
pip install git+https://github.com/ONSdigital/dp-data-pipelines.git
```

## Usage

The `dp-data-pipelines` package provides multiple data pipelines. Details of these can be found in the [`dpypelines` README](./dpypelines/README.md).

## Setup

Before you start using `dp-data-pipelines`, you need to configure your environment by logging in to AWS SSO (Amazon Web Services Single Sign On). This is necessary for pipeline functionality to access S3 buckets and process input files. To log in, open your terminal and enter the following command:

```bash
aws sso login --profile <AWS_PROFILE>
```

## Development

To ensure code quality, all Python code should follow `black` code formatting standards, with imports sorted according to `isort`. Code should also be linted according to the `ruff` linter. When raising a Pull Request, a GitHub Action will check that these tools have been run, as well as checking that all unit and acceptance tests pass. If any of these requirements are not met, the Action will fail.

For convenience, a Makefile is provided to make it simpler to run these utilities. The table below describes the available commands. To run any of these commands, open your terminal and enter the relevant command - for example, to run `black` and `isort`:

```bash
make fmt
```

| Command        | Description                                                                               |
|:---------------|:------------------------------------------------------------------------------------------|
| `make fmt`     | Runs `black` and `isort` on all Python files in the `dpypelines` and `tests` directories. |
| `make lint`    | Runs `ruff` on all Python files in the `dpypelines` and `tests` directories.              |
| `make test`    | Runs all unit tests.                                                                      |
| `make feature` | Runs all acceptance tests.                                                                |

## Configuration

The following environment variables are required for certain pipeline functionality to work correctly. To set environment variables, open your terminal and enter the following command:

```bash
export <ENVIRONMENT_VARIABLE_NAME>=<environment_variable_value>
```

| Environment variable  | Default | Description                                    | Usage                                                            |
|:----------------------|:-------:|:-----------------------------------------------|:-----------------------------------------------------------------|
| DE_SLACK_WEBHOOK      |  None   | Webhook for the Data Engineering Slack channel | Set **either** `DE_SLACK_WEBHOOK` **or** `DISABLE_NOTIFICATIONS` |
| DISABLE_NOTIFICATIONS |  False  | Toggle Slack notifications on or off           | Set **either** `DE_SLACK_WEBHOOK` **or** `DISABLE_NOTIFICATIONS` |
| AWS_PROFILE              |  None   | The AWS environment                                     |
| SKIP_DATA_UPLOAD         |  False  | Toggle upload functionality on or off                   | Set **either** `SKIP_DATA_UPLOAD` **or** `UPLOAD_SERVICE_URL` **and** `SERVICE_TOKEN_FOR_UPLOAD` |
| UPLOAD_SERVICE_URL       |  None   | The URL of the Upload Service                           | Set **either** `SKIP_DATA_UPLOAD` **or** `UPLOAD_SERVICE_URL` **and** `SERVICE_TOKEN_FOR_UPLOAD` |
| SERVICE_TOKEN_FOR_UPLOAD |  None   | The service token required to access the Upload Service | Set **either** `SKIP_DATA_UPLOAD` **or** `UPLOAD_SERVICE_URL` **and** `SERVICE_TOKEN_FOR_UPLOAD` |

Licence
-------

Copyright ©‎ 2024, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE) for details.