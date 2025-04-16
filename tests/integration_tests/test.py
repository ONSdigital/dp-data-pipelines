import json
import os

import pytest
import requests
from wiremock.client import Mappings

from wiremock.constants import Config
from wiremock.testing.testcontainer import wiremock_container
from dpypelines.pipeline.connectors.dataset_api.models import GetDatasetsResponse
from tests.integration_tests.setup.mappings import get_mappings

os.environ["SERVICE_TOKEN_FOR_UPLOAD"] = "testing"

@pytest.fixture(scope="module")
def wm_docker():
    with wiremock_container(verify_ssl_certs=False, secure=False) as wm:
        Config.base_url = wm.get_url("__admin")
        [Mappings.create_mapping(mapping=mapping) for mapping in get_mappings()]
        yield wm
        Mappings.delete_all_mappings()


def test_get_hello_world(wm_docker):
    print("TEST HERE")
    response = requests.get(wm_docker.get_url("/datasets"))

    assert response.status_code == 200
    serialised = GetDatasetsResponse.model_validate_json(response.content)
    
    assert serialised.count == 100
