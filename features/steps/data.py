from behave import *
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from pathlib import Path
import os
import shutil
from tempfile import TemporaryDirectory


@given(u'a temporary source directory of files')
def step_impl(context):
    files_to_retrieve = {}

    # Create a temporary directory to use for the test.
    # It will be automatically deleted at the end of the test.
    temporary_dir = TemporaryDirectory()
    temporary_dir_path = Path(temporary_dir.name)


    for row in context.table:
        # Populate a dictionary with the table contents from the context.
        file = row["file"].strip()
        fixture = row["fixture"].strip()
        files_to_retrieve[file] = fixture

        assert files_to_retrieve, f"No fixtures found to match input dictionary."

    fixtures_path = Path("fixtures/data/data-fixtures.zip")

    for root, dirs, files in os.walk(fixtures_path):
        for file in files: 
            if file in files_to_retrieve.values():
                fixture_path_data = fixtures_path / file

                with open(fixture_path_data) as f:
                    file_data = f.read()

                save_path = temporary_dir_path / file
                with open(save_path, "w") as f:
                    f.write(file_data)
        
    context.temporary_dir = temporary_dir

@given(u'v1_data_ingress starts using the temporary source directory')
def step_impl(context):

    dataset_ingress_v1(context.temporary_dir)