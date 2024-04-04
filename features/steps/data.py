from behave import *
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from pathlib import Path
import os
import pandas as pd
import json


@given("a temporary source directory of files")
def step_impl(context):
    files_to_retrieve = {}

    # Create a temporary directory to use for the test.
    context.temporary_directory = Path("temporary-data-fixtures").absolute()
    context.temporary_directory.mkdir()

    for row in context.table:
        # Populate a dictionary with the table contents from the context.
        file = row["file"].strip()
        fixture = row["fixture"].strip()
        files_to_retrieve[fixture] = file

        assert files_to_retrieve, f"No fixtures found to match input dictionary."

    fixtures_path = Path(Path(__file__).parent.parent / "fixtures/data/data-fixtures")

    for fixture, file in files_to_retrieve.items():

        with open(fixtures_path / fixture) as f1:
            file_object = f1.read()

            with open(context.temporary_directory / file, "w") as f2:
                f2.write(file_object)


@given("v1_data_ingress starts using the temporary source directory")
def step_impl(context):
    try:
        dataset_ingress_v1(context.temporary_directory)
        context.exception = None
    except Exception as exc:
        context.exception = exc


@then("the pipeline should generate no errors")
def step_impl(context):
    if context.exception is not None:
        raise context.exception

@given("I read the csv output 'data.csv'")
def step_impl(context):
    try:
        csv_output_path = context.temporary_directory / "data.csv"

        context.csv_output = pd.read_csv(csv_output_path)

    except Exception as exc:
        raise exc


@given("the csv output should have '100' rows")
def step_impl(context):
    num_rows = len(context.csv_output.index)

    assert num_rows == 100


@given("the csv output has the columns")
def step_impl(context):

    for column in context.table:
        assert column in context.csv_output.columns

    
@given("I read the metadata output '/outputs/metadata.json'")
def step_impl(context):
    try:
        json_output_path = context.temporary_directory / "metadata.json"

        context.json_output = json.load(json_output_path)

    except Exception as exc:
        raise exc


@given("the metadata should match 'cpih-metadata-correct.json'")
def step_impl(context):
    correct_metadata = json.load()

    assert context.json_output == correct_metadata

@then('the pipeline should generate an error with a message containing "{err_msg}"')
def step_impl(context, err_msg):
    assert (
        context.exception is not None
    ), "An error was expected but none was encountered"

    assert err_msg in str(
        context.exception
    ), f"""
        The expected string
        "{err_msg}"

        Was not found in the encountered exception:

        -----------------
        Exception follows
        -----------------

        {context.exception}

        -----------------
        """
