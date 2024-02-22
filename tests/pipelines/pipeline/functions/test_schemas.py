import pytest

from pipelines.pipeline.functions.schemas import get_config_schema_path


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
    assert "dp-data-pipelines/schemas/dataset-ingress/config/v1.json" in str(
        local_schema_path
    )
