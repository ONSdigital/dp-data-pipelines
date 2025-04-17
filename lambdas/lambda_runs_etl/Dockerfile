ARG ECR_REPOSITORY=""
ARG IMAGE_TO_USE="onsdigital/dp-concourse-tools-aws-lambda-python-git:3.13-git"
FROM ${ECR_REPOSITORY}${IMAGE_TO_USE} as lambda-base

RUN pip install poetry 
RUN poetry config virtualenvs.create false

WORKDIR ${LAMBDA_TASK_ROOT}

COPY pyproject.toml poetry.lock README.md ./

# Install dependencies without root package and without dev dependencies
RUN  poetry install --only=main --no-root --no-directory

COPY dpypelines ${LAMBDA_TASK_ROOT}/dpypelines
RUN poetry install --only main

FROM lambda-base as etl-lambda

WORKDIR ${LAMBDA_TASK_ROOT}

COPY --from=lambda-base /var/lang/lib/python3.13/site-packages /var/lang/lib/python3.13/site-packages
COPY --from=lambda-base ${LAMBDA_TASK_ROOT} ${LAMBDA_TASK_ROOT}

COPY lambdas/lambda_runs_etl/lambda_job_etl.py ${LAMBDA_TASK_ROOT}

CMD [ "lambda_job_etl.lambda_handler" ]