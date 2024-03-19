import pytest

from dpypelines.pipeline.shared.config import get_transform_identifier_from_config

def test_get_transform_identifier_from_config():
    config_dict = {
            "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v1.json",
            "options": {
                "transform_identifier": "my.cool.transform"
            }
        }
    pipeline = get_transform_identifier_from_config(config_dict)
    assert pipeline == "my.cool.transform"
    

def test_get_pipeline_identifier_from_config_incorrect_schema_id():
    config_dict = {
            "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v2.json",
            "options": {
                "transform_identifier": "my.cool.transform"
            }
        }
    with pytest.raises(NotImplementedError) as err:
        get_transform_identifier_from_config(config_dict)
        
    assert str(err.value) == f"{config_dict['$id']} is not a recognised $id"
    

def test_get_pipeline_identifier_from_config_incorrect_dict():
    config_dict = {
            "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v1.json"
        }
    with pytest.raises(AssertionError) as err:
        get_transform_identifier_from_config(config_dict)
        
    assert 'Config dict does not have expected root level options key' in str(err.value)


