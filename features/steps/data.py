from behave import *
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from pathlib import Path
import os
import shutil
from tempfile import TemporaryDirectory


@given("a temporary source directory of files")
def step_impl(context):
    files_to_retrieve = {}

    # Create a temporary directory to use for the test.
    context.temporary_directory = Path("features/temporary_fixture_dir")
    context.temporary_directory.mkdir()

    try:

        for row in context.table:
            # Populate a dictionary with the table contents from the context.
            file = row["file"].strip()
            fixture = row["fixture"].strip()
            files_to_retrieve[fixture] = file

            assert files_to_retrieve, f"No fixtures found to match input dictionary."

        fixtures_path = Path("features/fixtures/data")

        for _, _, files in os.walk(fixtures_path):
            for file in files:
                if file in files_to_retrieve.keys():
                    fixture_path_data = fixtures_path / file

                    with open(fixture_path_data) as f:
                        file_data = f.read()

                    save_path = context.temporary_directory / files_to_retrieve[file]
                    with open(save_path, "w") as f:
                        f.write(file_data)
    except Exception as exc:
        context.exception = exc


@given("v1_data_ingress starts using the temporary source directory")
def step_impl(context):
    try:
        dataset_ingress_v1(context.temporary_directory)
    except Exception as exc:
        context.exception = exc
