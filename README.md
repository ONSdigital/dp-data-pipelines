# dp-data-pipelines

Python scripts and tooling for automated website data ingress pipelines.

## Developer setup

### AWS

`dp-data-pipelines` relies on a number of AWS services to operate. You will need to log in to AWS via SSO (Single Sign On) to access S3 and Secrets Manager. To log in via AWS SSO, open your terminal and enter the following command:

```bash
aws sso login --profile <AWS_PROFILE>
```

### Python

We recommend using [pyenv](https://github.com/pyenv/pyenv) for Python version management. `dp-data-pipelines` uses Python 3.13.

### Poetry

We use [`poetry`](https://python-poetry.org/) to manage project dependencies. If you do not have `poetry` installed, you can install it by running `pip install poetry`. To install `dp-data-pipelines` dependencies, run `poetry install` from the root directory of the cloned repository.

### Environment configuration

Pipeline environment configuration is managed using AWS Secrets Manager and environment variables. See the [config](docs/config.md) documentation for further details. An example [`.env`](.env.example) file is also available. To use this, save a copy as `.env` in the root directory, and replace the values as necessary.

### Lambdas

The ingest pipeline depends on two AWS Lambda functions, which can be found in the [lambdas/][lambdas/] folder. View the [README](lambdas/README.md) for more information about these functions.

### Code quality

Repository conventions can be found in the [Conventions](CONVENTIONS.md) documentation.

<!---TODO reinstall isort?--->
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

### Docker

Docker is required for running integration tests. See the [Integration tests README](tests/integration/README.md#documentdbmongodb) for more information.

<!---TODO--->
<!---### Pre-commit--->

## Installation as a third-party package

This repository is also provided as an installable Python package. To install it, open your terminal and enter the following command:

```bash
pip install git+https://github.com/ONSdigital/dp-data-pipelines.git
```

## Additional documentation

Further documentation can be found in our [documentation folder](/docs/).

Licence
-------

Copyright ©‎ 2024, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE) for details.