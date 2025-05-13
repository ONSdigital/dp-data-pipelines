FROM python:3.11-alpine AS poetry

RUN pip install poetry

FROM poetry as base
COPY pyproject.toml poetry.lock README.md /app/
WORKDIR /app

RUN poetry config virtualenvs.create false
RUN poetry install --no-root --without dev

FROM base as copy

COPY --from=base /app /app
COPY lambdas/lambda_retry_etl /app/
COPY ./dpypelines /app/dpypelines

FROM copy as install

WORKDIR /app
COPY --from=base /app /app
RUN poetry install --no-root --without dev --only main --no-directory

FROM install
COPY --from=install /app /app

ENTRYPOINT [ "python" ]
CMD ["lambda_retry_ingest.py"]