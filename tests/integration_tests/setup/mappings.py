import json
from pydantic import BaseModel
from wiremock.client import HttpMethods, Mapping, MappingRequest, MappingResponse
from tests.integration_tests.setup.generators import generate_dataset

def get_datasets():
    return [
       generate_dataset().model_dump() for i in range(100)
    ]


def get_dataset_editions():
    return []


def create_response(items: list):
    return {"items": items, "count": len(items)}

def get_mappings() -> list[Mapping]:
    return [
        Mapping(
            priority=100,
            request=MappingRequest(method=HttpMethods.GET, url="/datasets"),
            response=MappingResponse(
                status=200, json_body=(create_response(get_datasets()))
            ),
            persistent=False,
        ),
        Mapping(
            priority=100,
            request=MappingRequest(
                method=HttpMethods.GET,
                url=r"/datasets",
                query_parameters={"state": {"equalTo": "published"}},
            ),
            response=MappingResponse(
                status=200,
                json_body=create_response(
                    list(filter(lambda p: p["state"] == "published", get_datasets())),
                ),
            ),
            persistent=False,
        ),
    ]
