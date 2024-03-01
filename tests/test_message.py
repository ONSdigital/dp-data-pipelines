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
    error = Exception("Some file is missing.")
    expected_message = """
        Something went wrong

        Error type: Exception
        Error: Some file is missing.
    """

    assert unexpected_error("Something went wrong", error) == expected_message


def test_cant_find_scheama():
    error = Exception("Something went wrong")
    config_dict = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"
    }

    human_readable_output = cant_find_scheama(config_dict, error)

    assert 'We got an error when trying to identify the schema for the pipeline-conifg.json using the pipeline-config.json.' in human_readable_output
    assert 'Pipeline-config.json:' in human_readable_output
    assert '"$schema": "http://json-schema.org/draft-04/schema#"' in human_readable_output
    assert '"$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"' in human_readable_output
    assert 'Error type: Exception' in human_readable_output
    assert 'Error: Something went wrong' in human_readable_output
    assert type(human_readable_output) == str


def test_invlaid_config():
    error = Exception("Something went wrong")
    config_dict = {
        "$schema": "http://json-schema.org/invalid/schema#",
        "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"
    }
    human_readable_output = invlaid_config(config_dict, error)
    
    assert "The pipeline config that was provided is failing to validate." in human_readable_output
    assert "Pipeline-config.json:" in human_readable_output
    assert '"$schema": "http://json-schema.org/invalid/schema#"' in human_readable_output
    assert '"$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"' in human_readable_output
    assert 'Error type: Exception' in human_readable_output
    assert "Error: Something went wrong" in human_readable_output
    assert type(human_readable_output) == str


def test_unknown_pipeline():
    
    pipeline_name = "sdmx.default"
    pipeline_options = {
        "pipeline idemtifier": "pipeline123"
    }

    human_readable_output = unknown_pipeline(pipeline_name, pipeline_options)

    assert 'Pipeline name is missing from the pipeline configurations.' in human_readable_output
    assert 'Pipeline: sdmx.default' in human_readable_output
    assert 'Pipeline Configurations:' in human_readable_output
    assert '"pipeline idemtifier": "pipeline123"' in human_readable_output
    assert type(human_readable_output) == str


def test_metadata_validation_error():
    error = Exception("Something went wrong")
    human_readable_output = metadata_validation_error("/some_parent_dir/some_dir/metadata.json", error)
    expected_output = """
        Metadata json file has failed validation: /some_parent_dir/some_dir/metadata.json
        Error type: Exception
        Error: Something went wrong
    """

    assert human_readable_output == expected_output
    assert type(human_readable_output) == str


def test_expected_local_file_missing():
    human_readable_output = expected_local_file_missing("Something went wrong", "/some_parent_dir/some_dir/some_json.json", "Pipeline123")
    expected_output = """
        A pipeline has encountered an issue finding a local file.
        
        Pipeline's name: Pipeline123
        File: /some_parent_dir/some_dir/some_json.json
        Message: Something went wrong
    """
    assert human_readable_output == expected_output
    assert type(human_readable_output) == str