FROM python:3.11-alpine
WORKDIR /app

# install pymongo
RUN pip install pymongo

COPY ./ ./
CMD ["./lambda_retry_ingest.py"]