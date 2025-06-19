# dp-data-pipelines

Python scripts and tooling for automated website data ingress pipelines.

## Installation

This repository is provided as an installable Python package. To install it, open your terminal and enter the following command:

```bash
pip install git+https://github.com/ONSdigital/dp-data-pipelines.git
```

## Setup

Before you start using `dp-data-pipelines`, you need to configure your environment by logging in to AWS SSO (Single Sign On). This is necessary for pipeline functionality to access S3 buckets and process input files. To log in, open your terminal and enter the following command:

```bash
aws sso login --profile <AWS_PROFILE>
```

## Python

We recommend using [pyenv](https://github.com/pyenv/pyenv) for Python version management. `dp-data-pipelines` uses Python 3.13.

## Poetry

We use [`poetry`](https://python-poetry.org/) to manage project dependencies. If you do not have `poetry` installed, you can install it by running `pip install poetry`. To install `dp-data-pipelines` dependencies, run `poetry install` from the root directory.

## Development

***TODO reinstall isort?***
To ensure code quality, all Python code should follow `ruff` code formatting standards. Code should also be linted according to the `ruff` linter. When raising a Pull Request, a GitHub Action will check that all code meets these quality standards, as well as checking that all unit and integration tests pass. If any of these requirements are not met, the Action will fail.

For convenience, a Makefile is provided to make it simpler to run these utilities. The table below describes the available commands. To run any of these commands, open your terminal and enter the relevant command - for example, to run `ruff format`:

```bash
make fmt
```

| Command                  | Description                                                    |
|:-------------------------|:---------------------------------------------------------------|
| `make fmt`               | Runs `ruff format` on all Python files in the repository.      |
| `make lint`              | Runs `ruff check --fix` on all Python files in the repository. |
| `make test`              | Runs all unit and integration tests.                           |
| `make tests-integration` | Runs all integration tests.                                    |
| `make tests-unit`        | Runs all unit tests.                                           |
| `make deploy`            | Builds `dpypelines` wheel and uploads to AWS S3 bucket.        |
| `make symlink`           | Switches to local `dpytools` version.                          |
| `make unlink`            | Switches to production `dpytools` version.                     |

## Configuration

Configuration is managed using AWS Secrets Manager and environment variables. See the [config documentation](docs/config.md) for further details.

## Lambdas

The repository is currently used by two separate Lambdas, which are in the [lambdas/][lambdas/] folder. View the [README](lambdas/README.md) for further information about them (their purpose, what they do, etc.).

## Docker/Podman

***TODO Docker for integration tests***

# Pre-commit

***TODO is pre-commit configured properly?***

## Additional documentation

Further documentation can be found in our [documentation folder](/docs/)

Licence
-------

Copyright ©‎ 2024, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE) for details.