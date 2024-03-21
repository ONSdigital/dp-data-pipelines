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
    temporary_dir = Path("features/temporary_fixture_dir")
    temporary_dir.mkdir()

    try:

        for row in context.table:
            # Populate a dictionary with the table contents from the context.
            file = row["file"].strip()
            fixture = row["fixture"].strip()
            files_to_retrieve[fixture] = file

            assert files_to_retrieve, f"No fixtures found to match input dictionary."

        fixtures_path = Path("features/fixtures/data")

        for root, dirs, files in os.walk(fixtures_path):
            for file in files:
                if file in files_to_retrieve.keys():
                    fixture_path_data = fixtures_path / file

                    with open(fixture_path_data) as f:
                        file_data = f.read()

                    save_path = temporary_dir / files_to_retrieve[file]
                    with open(save_path, "w") as f:
                        f.write(file_data)

        context.temporary_dir = temporary_dir

    # If anything goes wrong, make sure the temporary directory gets removed.
    except Exception:
        shutil.rmtree(temporary_dir)


@given("v1_data_ingress starts using the temporary source directory")
def step_impl(context):

    try:
        dataset_ingress_v1(context.temporary_dir)
        shutil.rmtree(context.temporary_dir)
    except Exception:
        shutil.rmtree(context.temporary_dir)
