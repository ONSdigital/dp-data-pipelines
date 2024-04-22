import pytest
from dpytools.stores.directory.local import LocalDirectoryStore


from dpypelines.pipeline.shared.message import (
    cant_find_schema,
    error_in_transform,
    expected_local_file_missing,
    invalid_config,
    metadata_validation_error,
    pipeline_input_exception,
    pipeline_input_sanity_check_exception,
    unexpected_error,
    unknown_transform,
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
        "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json",
    }

    human_readable_output = cant_find_schema(config_dict, error)

    assert (
        "We got an error when trying to identify the schema for the pipeline-config.json using the pipeline-config.json."
        in human_readable_output
    )
    assert "Pipeline-config.json:" in human_readable_output
    assert (
        '"$schema": "http://json-schema.org/draft-04/schema#"' in human_readable_output
    )
    assert (
        '"$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"'
        in human_readable_output
    )
    assert "Error type: Exception" in human_readable_output
    assert "Error: Something went wrong" in human_readable_output
    assert isinstance(human_readable_output, str)


def test_invalid_config():
    error = Exception("Something went wrong")
    config_dict = {
        "$schema": "http://json-schema.org/invalid/schema#",
        "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json",
    }
    human_readable_output = invalid_config(config_dict, error)

    assert (
        "The pipeline config that was provided is failing to validate."
        in human_readable_output
    )
    assert "Pipeline-config.json:" in human_readable_output
    assert (
        '"$schema": "http://json-schema.org/invalid/schema#"' in human_readable_output
    )
    assert (
        '"$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"'
        in human_readable_output
    )
    assert "Error type: Exception" in human_readable_output
    assert "Error: Something went wrong" in human_readable_output
    assert isinstance(human_readable_output, str)


def test_unknown_transform():

    transform_name = "sdmx.default"
    all_transform_options = {"pipeline idemtifier": "pipeline123"}

    human_readable_output = unknown_transform(transform_name, all_transform_options)

    assert (
        "Pipeline name is missing from the pipeline transform configurations."
        in human_readable_output
    )
    assert "Pipeline: sdmx.default" in human_readable_output
    assert "Pipeline Configurations:" in human_readable_output
    assert '"pipeline idemtifier": "pipeline123"' in human_readable_output
    assert isinstance(human_readable_output, str)


def test_metadata_validation_error():
    error = Exception("Something went wrong")
    human_readable_output = metadata_validation_error(
        "/some_parent_dir/some_dir/metadata.json", error
    )
    expected_output = """
        Metadata json file has failed validation: /some_parent_dir/some_dir/metadata.json
        Error type: Exception
        Error: Something went wrong
    """

    assert human_readable_output == expected_output
    assert isinstance(human_readable_output, str)


def test_expected_local_file_missing():
    store_path = "tests/fixtures/test-cases/message_test_directory"
    test_input_store = LocalDirectoryStore(store_path)

    human_readable_output = expected_local_file_missing(
        "Something went wrong",
        "/some_parent_dir/some_dir/some_json.json",
        "Pipeline123",
        test_input_store,
    )
    expected_output = """
        A pipeline has encountered an issue finding a local file.
        
        Pipeline's name: Pipeline123
        File: /some_parent_dir/some_dir/some_json.json
        Message: Something went wrong
        Files in directory store:
        test_local_file.json
    """
    assert human_readable_output == expected_output
    assert isinstance(human_readable_output, str)


def test_pipeline_input_exception():
    """
    Checks that the error message for a pipeline input exception contains
    the expected message text and details.
    """
    store_path = "tests/fixtures/test-cases/message_test_directory"
    test_input_store = LocalDirectoryStore(store_path)

    pipeline_dict = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json",
    }

    error = Exception("Something went wrong during the pipeline input process")
    human_readable_output = pipeline_input_exception(
        pipeline_dict, test_input_store, error
    )

    assert (
        """File specified in pipeline details could not be retrieved from store: tests/fixtures/test-cases/message_test_directory
        Error type: Exception
        Error: Something went wrong during the pipeline input process"""
        in human_readable_output
    )
    assert (
        '"$schema": "http://json-schema.org/draft-04/schema#"' in human_readable_output
    )
    assert (
        '"$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"'
        in human_readable_output
    )

    assert isinstance(human_readable_output, str)


def test_error_in_transform():
    """
    Checks that the error message for an error in the transformation process
    contains the expected message text and details.
    """
    store_path = "tests/fixtures/test-cases/message_test_directory"
    test_input_store = LocalDirectoryStore(store_path)

    pipeline_dict = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json",
    }

    error = Exception("Something went wrong during the transformation process")
    human_readable_output = error_in_transform(pipeline_dict, test_input_store, error)

    assert (
        """An error occured during the transformation process for pipeline input with store: tests/fixtures/test-cases/message_test_directory
        Error type: Exception
        Error: Something went wrong during the transformation process"""
        in human_readable_output
    )
    assert (
        '"$schema": "http://json-schema.org/draft-04/schema#"' in human_readable_output
    )
    assert (
        '"$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"'
        in human_readable_output
    )

    assert isinstance(human_readable_output, str)


def test_pipeline_input_sanity_check_exception():
    """
    Checks that the error message for an error during the sanity check of the
    input contains the expected message text and details.
    """
    store_path = "tests/fixtures/test-cases/message_test_directory"
    test_input_store = LocalDirectoryStore(store_path)

    pipeline_dict = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json",
    }

    error = Exception("Something went wrong during the pipeline input sanity check")
    human_readable_output = pipeline_input_sanity_check_exception(
        pipeline_dict, test_input_store, error
    )

    assert (
        """An error was raised while performing a sanity check on pipeline with given store: tests/fixtures/test-cases/message_test_directory
        Error type: Exception
        Error: Something went wrong during the pipeline input sanity check"""
        in human_readable_output
    )
    assert (
        '"$schema": "http://json-schema.org/draft-04/schema#"' in human_readable_output
    )
    assert (
        '"$id": "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"'
        in human_readable_output
    )

    assert isinstance(human_readable_output, str)
