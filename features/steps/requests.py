import json
from behave import *
from pathlib import Path
from urllib3.util import parse_url


def _parse_destination_url_from_log(log: str) -> str:
    """
    Get the receiving path from the docker log
    """
    path_line = [x for x in log.split("\n") if "this-requests-url" in x]
    assert len(path_line) == 1, f"Cannot find 'this-requests-url' in service log {log}"
    destination_url = path_line[0].split("this-requests-url: ")[1]
    return parse_url(destination_url)[4]


def _parse_request_body_from_log(log: str) -> str:
    body_line = [x for x in log.split("\n") if "this-requests-body" in x]
    assert len(body_line) == 1, f"Cannot find 'this-requests-body' in service log {log}"
    request_body = body_line[0].split("this-requests-body: ")[1]
    return request_body


def _parse_dict_from_log(log: str, descriptor: str) -> dict:
    log_line = [x for x in log.split("\n") if descriptor in x]
    assert len(log_line) == 1, f"Cannot find '{descriptor}' in service log {log}"
    request_str = log_line[0].split(descriptor + ": ")[1].replace("'", '"')
    request_dict = json.loads(request_str)
    return request_dict


def get_request_logs(container, request_id) -> str:
    """
    Get docker logs for this request as one big string.
    """
    # Get logs relevant to this specific request
    docker_logs = str(container.logs().decode("utf-8"))
    assert (
        request_id in docker_logs
    ), f"""
        Could not find: {request_id} in:
        {str(docker_logs)}
        """
    return docker_logs.split(request_id)[1]


@Given('a request json payload of "{request_json_payload}"')
def step_impl(context, request_json_payload):
    """
    Load a JSON payload file to submit as part of the request.
    """
    relative_features_path = Path(__file__).parent.parent

    request_json_payload_path = relative_features_path / request_json_payload

    with open(request_json_payload_path, "r") as f:
        context.json = json.load(f)


@Given("a request with the headers")
def step_impl(context):
    """
    Set request headers from table in scenario.
    """
    headers = {}
    for row in context.table:
        key = row["key"].strip()
        value = row["value"].strip()
        headers[key] = value
    context.request_headers = headers


@Given('I send the request to the upload service at "{service_endpoint}"')
def step_impl(context, service_endpoint):
    """
    Submit a GET request to the specified endpoint.
    """
    context.request_path = service_endpoint
    context.response = context.session.get(
        f"http://127.0.0.1:5001{service_endpoint}",
        headers=context.request_headers,
        json=context.json,
    )


@Then('the backend receives a request to "{service_endpoint}"')
def step_impl(context, service_endpoint):
    """
    Check that the path in question appears in the logs
    of the receiving service.
    """
    context.receiving_service_log = get_request_logs(
        context.backend_container, context.request_id
    )
    destination_url = _parse_destination_url_from_log(context.receiving_service_log)
    assert (
        service_endpoint in destination_url
    ), f"""
        Cannot find path "{service_endpoint}" in destination url: {destination_url}.
        """


@Then('the csv payload received should contain "{request_csv_payload}"')
def step_impl(context, request_csv_payload):
    """
    Ensure that the request body contains the CSV data specified in the test file.
    """
    request_body = _parse_request_body_from_log(context.receiving_service_log)

    assert request_csv_payload in request_body


@Then('the json payload received should match "{request_json_payload}"')
def step_impl(context, request_json_payload):
    """
    Ensure that the request body contains the JSON data specified in the test file.
    """
    request_json_dict = _parse_dict_from_log(
        context.receiving_service_log, "this-requests-json"
    )
    relative_features_path = Path(__file__).parent.parent

    request_json_payload_path = relative_features_path / request_json_payload

    with open(request_json_payload_path, "r") as f:
        data = json.load(f)
    assert data == request_json_dict


@Then("the headers received should match")
def step_impl(context):
    """
    Use the 'this-requests-headers' text from the
    docker logs to pull our one json entries worth of headers and
    parse it back into a python dictionary.
    """
    headers_as_dict = _parse_dict_from_log(
        context.receiving_service_log, "this-requests-headers"
    )
    # Check that the headers dict contains the expected key-value pairs
    for row in context.table:
        key = row["key"].strip()
        value = row["value"].strip()
        assert key in headers_as_dict, f'No "{key}" header found in {headers_as_dict}'
        assert (
            value in headers_as_dict[key]
        ), f'Expecting header "{key}" to have value "{value}, but got "{headers_as_dict[key]}"'
