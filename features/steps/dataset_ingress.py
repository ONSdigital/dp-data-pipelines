from pathlib import Path

from behave import *

from dictdiffer import diff
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.generic_file_ingress_v1 import generic_file_ingress_v1
from dpypelines.pipeline.shared.transforms.sdmx.v1 import (
    sdmx_compact_2_0_prototype_1,
    sdmx_compact_2_1_prototype,
    sdmx_sanity_check_v1,
)

CONFIGURATION = {
    "valid": {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$"}],
        "supplementary_distributions": [{"matches": "^data.xml$"}],
        "secondary_function": dataset_ingress_v1,
    },
    "valid_no_supp_dist_2_0": {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$"}],
        "supplementary_distributions": [],
        "secondary_function": dataset_ingress_v1,
    },
    "valid_no_supp_dist_2_1": {
        "config_version": 1,
        "transform": sdmx_compact_2_1_prototype,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$"}],
        "supplementary_distributions": [],
        "secondary_function": dataset_ingress_v1,
    },
    "valid_generic_file_ingress": {
        "config_version": 1,
        "transform": None,
        "transform_inputs": {},
        "transform_kwargs": {},
        "required_files": [{"matches": "^(?!manifest.json$)"}],
        "supplementary_distributions": [],
        "secondary_function": generic_file_ingress_v1,
    },
    "invalid": {
        "config_version": 2,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$"}],
        "supplementary_distributions": [{"matches": "^data.xml$"}],
        "secondary_function": dataset_ingress_v1,
    },
}
import json

import pandas as pd


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


@given("a dataset id of '{source_id}'")
def step_impl(context, source_id):
    context.pipeline_config = CONFIGURATION[source_id]


@given("dataset_ingress_v1 starts using the temporary source directory")
def step_impl(context):
    try:
        dataset_ingress_v1(
            context.temporary_directory.absolute(), context.pipeline_config
        )
        context.exception = None
    except Exception as exc:
        context.exception = exc


@given("generic_file_ingress_v1 starts using the temporary source directory")
def step_impl(context):
    try:
        generic_file_ingress_v1(
            context.temporary_directory.absolute(), context.pipeline_config
        )
        context.exception = None
    except Exception as exc:
        context.exception = exc


@then("the pipeline should generate no errors")
def step_impl(context):
    if context.exception is not None:
        raise context.exception


@then("I read the csv output '{csv_output}'")
def step_impl(context, csv_output):
    context.csv_output = pd.read_csv(csv_output)

@then("I read the xml output '{xml_output}'")
def step_impl(context, xml_output):
    with open(context.temporary_directory / xml_output, "r") as f:
        context.xml_content = f.read()


@then("the csv output should have '{number}' rows")
def step_impl(context, number):
    num_rows = len(context.csv_output.index)
    assert num_rows == int(number), f"Csv should have {number} rows but has {num_rows}"


@then("the xml output should have length '{length}'")
def step_impl(context, length):
    xml_length = len(context.xml_content)
    assert xml_length == int(
        length
    ), f"XML should have length {length}, but has length {xml_length}"


@then("the csv output has the columns")
def step_impl(context):
    test_table_cols = context.table.headings
    for column in test_table_cols:
        assert (
            column in context.csv_output.columns
        ), f"Column {column} does not match any expected columns: {context.csv_output.columns}"


@then("the xml output contains '{xml}'")
def step_impl(context, xml):
    assert (
        xml in context.xml_content
    ), f"XML should contain {xml} but this is not present"


@then("I read the metadata output '{metadata_output}'")
def step_impl(context, metadata_output):
    json_output_path = Path(metadata_output)

    metadata_file = open(json_output_path)

    context.json_output = json.load(metadata_file)


@then("the metadata should match '{correct_metadata}'")
def step_impl(context, correct_metadata):
    relative_features_path = Path(__file__).parent.parent

    correct_metadata_path = Path(relative_features_path / correct_metadata)
    correct_metadata_file = open(correct_metadata_path)
    correct_metadata_json = json.load(correct_metadata_file)

    result = diff(context.json_output, correct_metadata_json)
    assert (
        context.json_output == correct_metadata_json
    ), f"Metadata does not match expected metadata, (`add` means values are missing, `remove` means values need to be deleted, `change` means correct amount of values but doesn't match) :\n {list(result)}."


@then("the metadata should not match '{incorrect_metadata}'")
def step_impl(context, incorrect_metadata):
    relative_features_path = Path(__file__).parent.parent

    incorrect_metadata_path = Path(relative_features_path / incorrect_metadata)
    incorrect_metadata_file = open(incorrect_metadata_path)
    incorrect_metadata_json = json.load(incorrect_metadata_file)

    result = diff(context.json_output, incorrect_metadata_json)
    assert (
        context.json_output != incorrect_metadata_json
    ), "Got matching metadata result when there should be no match."


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
