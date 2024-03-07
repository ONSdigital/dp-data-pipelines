import pytest

from dpypelines.pipeline.functions.schemas import get_config_schema_path


def test_get_config_schema_path():
    config = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v1.json",
        "required_files": [{"matches": "*.sdmx", "count": "1"}],
        "supplementary_distributions": [{"matches": "*.sdmx", "count": "1"}],
        "priority": "1",
        "pipeline": "sdmx.default",
    }
    local_schema_path = get_config_schema_path(config)
    assert "dp-data-pipelines/dpypelines/schemas/dataset-ingress/config/v1.json" in str(
        local_schema_path
    )


def test_get_config_schema_path_no_config_id():
    config = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "required_files": [{"matches": "*.sdmx", "count": "1"}],
        "supplementary_distributions": [{"matches": "*.sdmx", "count": "1"}],
        "priority": "1",
        "pipeline": "sdmx.default",
    }
    with pytest.raises(KeyError) as e:
        get_config_schema_path(config)
    assert "No `$id` field in config" in str(e.value)


def test_get_config_schema_path_no_local_schema():
    config = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/no-local-schema.json",
        "required_files": [{"matches": "*.sdmx", "count": "1"}],
        "supplementary_distributions": [{"matches": "*.sdmx", "count": "1"}],
        "priority": "1",
        "pipeline": "sdmx.default",
    }
    with pytest.raises(FileNotFoundError) as e:
        get_config_schema_path(config)
    assert (
        "Local schema not found from $id 'https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/no-local-schema.json'"
        in str(e.value)
    )
