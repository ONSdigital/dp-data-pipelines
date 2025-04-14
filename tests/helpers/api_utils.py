import os
import random
from typing import List

import requests
from requests.exceptions import HTTPError

from dpypelines.pipeline.connectors.dataset_api.models import (
    Dataset,
    DatasetApiConfig,
    Edition,
    GetDatasetEditionsResponse,
    GetDatasetsResponse,
)


def get_dataset_api_config() -> DatasetApiConfig:
    url = os.environ["DATASET_API_URL"]
    auth_token = os.environ["DP_AUTH_TOKEN"]

    return DatasetApiConfig(url, auth_token)


def get_auth_headers(config: DatasetApiConfig):
    return {"Authorization": f"Bearer {config.auth_token}"}


def get_static_datasets():
    try:
        config = get_dataset_api_config()

        params = {"type": "static"}

        headers = get_auth_headers(config)

        response = requests.get(config.url, params=params, headers=headers)
        response.raise_for_status()

        content = response.json()

        return GetDatasetsResponse(**content)
    except HTTPError as e:
        print(
            f"Error getting static datasets. Status code {e.response.status_code}, response body:\n\n{e.response.content}"
        )


def filter_datasets_by_state(state: str, datasets: List[Dataset]) -> List[Dataset]:
    return [
        dataset
        for dataset in datasets
        if dataset.current is not None and dataset.current.state == state
    ]


def get_published_datasets():
    datasets = get_static_datasets()
    filtered = filter_datasets_by_state("published", datasets.items)
    return filtered


def get_dataset_editions(dataset_id: str):
    try:
        config = get_dataset_api_config()

        headers = get_auth_headers(config)

        response = requests.get(f"{config.url}/{dataset_id}/editions", headers=headers)
        response.raise_for_status()

        content = response.json()

        return GetDatasetEditionsResponse(**content)
    except HTTPError as e:
        print(
            f"Error getting static datasets. Status code {e.response.status_code}, response body:\n\n{e.response.content}"
        )


class DatasetWithEdition:
    dataset: Dataset
    edition: Edition

    def __init__(self, dataset: Dataset, edition: Edition):
        self.dataset = dataset
        self.edition = edition


def get_random_dataset_with_edition() -> DatasetWithEdition:
    published_datasets = get_published_datasets()
    dataset_to_use = random.randint(0, len(published_datasets))
    dataset = published_datasets[dataset_to_use]
    editions = get_dataset_editions(dataset.id)

    if len(editions.items) == 0:
        raise Exception(f"Could not find editions for dataset {dataset.id}")

    return DatasetWithEdition(dataset, editions.items[0])
