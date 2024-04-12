from behave import *
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from pathlib import Path
import os

from dpypelines.pipeline.shared.transforms.sdmx.v1 import (
    sdmx_compact_2_0_prototype_1,
    sdmx_sanity_check_v1,
)


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

    fixtures_path = Path(Path(__file__).parent.parent / "fixtures/data")

    for fixture, file in files_to_retrieve.items():

        with open(fixtures_path / fixture) as f1:
            file_object = f1.read()

            with open(context.temporary_directory / file, "w") as f2:
                f2.write(file_object)


@given("a valid pipeline configuration dictionary")
def step_impl(context):
    context.pipeline_config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }


@given("an invalid pipeline configuration dictionary")
def step_impl(context):
    context.pipeline_config = {
        "config_version": 2,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }


@given("v1_data_ingress starts using the temporary source directory")
def step_impl(context):
    try:
        dataset_ingress_v1(context.temporary_directory, context.pipeline_config)
        context.exception = None
    except Exception as exc:
        context.exception = exc


@then("the pipeline should generate no errors")
def step_impl(context):
    if context.exception is not None:
        raise context.exception


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
