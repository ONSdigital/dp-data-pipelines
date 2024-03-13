import pytest

from dpypelines.pipeline.shared.config import get_pipeline_identifier_from_config

def test_get_pipeline_identifier_from_config():
    config_dict = {
            "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v1.json",
            "pipeline": "dataset_ingress_v1.yml"
        }
    pipeline = get_pipeline_identifier_from_config(config_dict)
    assert pipeline == "dataset_ingress_v1.yml"
    

def test_get_pipeline_identifier_from_config_incorrect_schema_id():
    config_dict = {
            "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v2.json",
            "pipeline": "dataset_ingress_v1.yml"
        }
    with pytest.raises(NotImplementedError) as err:
        get_pipeline_identifier_from_config(config_dict)
        
    assert str(err.value) == f"{config_dict['$id']} is not a recognised $id"
    

def test_get_pipeline_identifier_from_config_incorrect_dict():
    config_dict = {
            "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v1.json"
        }
    with pytest.raises(AssertionError) as err:
        get_pipeline_identifier_from_config(config_dict)
        
    assert 'pipeline is not in the config dict' in str(err.value)


