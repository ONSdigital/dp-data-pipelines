import pytest
from pipeline.shared.functions.message import (
    unexpected_error, 
    cant_find_scheama,
    invlaid_config,
    unknown_pipeline,
    metadata_validation_error,
    expected_local_file_missing
)


def test_unexpected_error():
    try:
        assert 1 == 2
    except Exception as e:
        human_readable_output = unexpected_error(e.message, e)

    # assert human_readable_output == "An unexpected error occured with error message: {msg}"
    assert type(human_readable_output) == str


def test_cant_find_scheama():
    try:
        config_dict = {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json",
            "required_files": [
                {
                    "matches": "*.sdmx",
                    "count": "1"
                }
            ],
            "supplementary_distributions": [
                {
                    "matches": "*.sdmx",
                    "count": "1"
                }
            ],
            "priority": "1",
            "pipeline": "sdmx.default" 
        }
        assert config_dict["$schema"] == 2
    except Exception as e:
        human_readable_output = cant_find_scheama(config_dict, e)

    # assert human_readable_output == "An unexpected error occured with error message: {msg}"
    assert type(human_readable_output) == str


def test_invlaid_config():
    try:
        config_dict = {
            "$schema": "http://json-schema.org/invalid/schema#",
            "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json",
            "required_files": [
                {
                    "matches": "*.sdmx",
                    "count": "1"
                }
            ],
            "supplementary_distributions": [
                {
                    "matches": "*.sdmx",
                    "count": "1"
                }
            ],
            "priority": "1",
            "pipeline": "sdmx.default" 
        }
        assert config_dict["$schema"] == "http://json-schema.org/draft-04/schema#"
    except Exception as e:
        human_readable_output = invlaid_config(config_dict, e)

    # assert human_readable_output == "An unexpected error occured with error message: {msg}"
    assert type(human_readable_output) == str


def test_unknown_pipeline():
    try:
        all_pipeline_details = {
            "sdmx.default": {
                "transform": "smdx_default_v1",
                "transform_inputs": {
                    "*.sdmx": "sdmx_sanity_check_v1"
                },
                "transform_kwargs": {}
            }
        }
        pipeline_name = all_pipeline_details[0]
        pipeline_options = {
            "pipeline idemtifier": "pipeline123"
        }
        assert pipeline_name == pipeline_options["pipeline idemtifier"]
    except Exception as e:
        human_readable_output = unknown_pipeline(pipeline_name, pipeline_options)

    # assert human_readable_output == "An unexpected error occured with error message: {msg}"
    assert type(human_readable_output) == str


def test_metadata_validation_error():
    try:
        metadata = {}
        metadata_path = "/some_parent_dir/some_dir/metadata.json"
        assert metadata == True
    except Exception as e:
        human_readable_output = metadata_validation_error(metadata_path, e)

    # assert human_readable_output == "An unexpected error occured with error message: {msg}"
    assert type(human_readable_output) == str


def test_expected_local_file_missing():
    try:
        pipeline_name = "Pipeline123"
        file_path = "/some_parent_dir/some_dir/some_json.json"
        assert 1 == 2
    except Exception as e:
        human_readable_output = expected_local_file_missing(e.message, file_path, pipeline_name)

    # assert human_readable_output == "An unexpected error occured with error message: {msg}"
    assert type(human_readable_output) == str