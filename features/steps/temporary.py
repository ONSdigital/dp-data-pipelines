from pathlib import Path
from behave import Given, Then


def _parse_destination_url_from_log(log: str) -> str:
    """
    Get the receiving path from the docker log
    """
    path_line = [x for x in log.split("\n") if "this-requests-url" in x]
    assert len(path_line) == 1, f'Cannot find "this-requests-url" in service log {log}'
    return "/" + path_line[0].split("/")[-1].strip()
def _parse_request_headers_as_dict_from_log(log: str) -> dict:
    """
    Get the headers from the docker logs as a dictionary.
    """
    assert "this-requests-headers:" in log, (
        "'this-requests-headers:' expected but not found in: " f"{log}"
    )
    headers_dict_as_str = (
        log.split("this-requests-headers:")[1].replace("'", '"').split("}")[0] + "}"
    )
    headers_as_dict = json.loads(headers_dict_as_str)
    return headers_as_dict
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
    context.receiving_service_log = get_request_logs(
        context.backend_container, context.request_id
    )
@Then('the backend receives a request to "{service_endpoint}"')
def step_impl(context, service_endpoint):
    """
    Check that the path in question appears in the logs
    of the receiving service.
    """
    destination_url = _parse_destination_url_from_log(context.receiving_service_log)
    assert (
        service_endpoint in destination_url
    ), f"""
        Cannot find path "{service_endpoint}" in destination url: {destination_url}.
        """
@Then('the json payload received should match "{request_json_payload}"')
def step_impl(context, request_json_payload):
    """
    Ensure that the request body contains the JSON data specified in the test file.
    """
    request_body = context.response.request.body.decode("utf-8")
    relative_features_path = Path(__file__).parent.parent

    request_json_payload_path = relative_features_path / request_json_payload

    with open(request_json_payload_path, "r") as f:
        data = json.load(f)
    assert data == json.loads(request_body)

@Then("the headers received should match")
def step_impl(context):
    """
    Use the 'this-requests-headers' text from the
    docker logs to pull our one json entries worth of headers and
    parse it back into a python dictionary.
    """
    headers_as_dict = _parse_request_headers_as_dict_from_log(
        context.receiving_service_log
    )
    # Check that the headers dict contains the expected key-value pairs
    for row in context.table:
        key = row["key"].strip()
        value = row["value"].strip()
        assert key in headers_as_dict, f'No "{key}" header found in {headers_as_dict}'
        assert (
            value in headers_as_dict[key]
), f'Expecting header "{key}" to have value "{value}, but got "{headers_as_dict[key]}"'