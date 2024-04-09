import shutil
from behave import fixture
from zipfile import ZipFile
from pathlib import Path
import os
import uuid
import time
import requests
import docker
from docker import DockerClient


def before_all(context):
    """
    Before running the tests we set up a docker container representing a fake backend.

    This is a simple flask app that logs out
    the details of requests received - this is enough for us
    to confirm that a request is routing as required via the feature
    files.

    Also checks if the fixtures unzipped data path exists,
    and if it doesn't, extracts the fixtures zipfile to it.
    """
    context.features_directory = Path(__file__).parent

    context.fixture_destination_path = Path(context.features_directory / "fixtures/data")

    zip_path = Path("features/fixtures/data-fixtures.zip")

    if not context.fixture_destination_path.exists():
        os.mkdir(context.fixture_destination_path)

        with ZipFile(zip_path, "r") as fixtures_zip:
            fixtures_zip.extractall(path=context.fixture_destination_path)


    docker_client: DockerClient = docker.from_env()
    repo_root = "/".join(str(Path(__file__).absolute()).split("/")[:-2])

    backend_dir = repo_root + "/features/docker/fake_backend"

    # Backend service
    docker_client.images.build(
        path=backend_dir, nocache=True, tag="fake_backend:latest"
    )
    context.backend_container = docker_client.containers.run(
        "fake_backend:latest",
        name="fake_backend",
        ports={"5001": "5001"},
        publish_all_ports=True,
        network_mode="bridge",
        detach=True,
    )
    container_info = docker_client.containers.get("fake_backend")
    context.backend_ip = container_info.attrs["NetworkSettings"]["Networks"]["bridge"][
        "IPAddress"
    ]
    time.sleep(10)


def before_scenario(context, scenario):
    """
    Test setup that runs at the start of each individual scenario.
    """

    # Change directory to a temporary directory to accomodate any test files being dumped to the "current directory"
    # This allows us to safely delete them later.
    context.temp_test_dir = Path("temporary_test_output/")
    os.mkdir(context.temp_test_dir)
    os.chdir(context.temp_test_dir)

    # Set temporary_directory on context (value populated in relevant step definition)
    context.temporary_directory = None

    # Stick a uuid in the docker logs for all services,
    # so we can demarcate (split) the docker logs later
    # to check the things we expect should have been
    # logged out actually have been.
    context.request_id = str(uuid.uuid4())
    requests.get(
        f"http://127.0.0.1:5001/request-ids/{context.request_id}", data=scenario.name
    )

    # By default requests sets some (entirely sensible)
    # headers on all requests which can lead to some
    # unexpected results in the context of testing.
    # So we're going to always start each scenario with
    # a custom session with all such defaults removed.
    context.session = requests.Session()
    context.session.headers = {}


def after_scenario(context, scenario):
    # Remove temporary directory and any files within it
    if context.temporary_directory is not None:
        shutil.rmtree(context.temporary_directory)

    # Change out of temporary directory and delete its contents from finished scenario
    os.chdir("..")
    shutil.rmtree(context.temp_test_dir)


def after_all(context):
    """
    Stop and remove the docker container after all tests have finished running.
    Also remove the temporary directories for test output files.
    """
    context.backend_container.stop()
    context.backend_container.remove()

    if context.fixture_destination_path is not None:
        shutil.rmtree(context.fixture_destination_path.absolute())
