import pytest
import sys

sys.path.append("../../../..") # path to dp-data-pipelines folder
from pipelines.pipeline.functions.config import get_pipeline_identifier_from_config

def test_get_pipeline_identifier_from_config():
    config_dict = {
            "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v1.json",
            "pipeline": "pipeline-dataset-ingress-v1.yml"
        }
    get_pipeline_identifier_from_config(config_dict)
    

def test_get_pipeline_identifier_from_config_incorrect_schema_id():
    config_dict = {
            "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v2.json",
            "pipeline": "pipeline-dataset-ingress-v1.yml"
        }
    with pytest.raises(NotImplementedError) as err:
        get_pipeline_identifier_from_config(config_dict)
        
    assert str(err.value) == "Only currently built to accept '/schemas/dataset-ingress/config/v1.json' as valid $id"


def test_get_pipeline_identifier_from_config_incorrect_dict():
    config_dict = {
            "$id": "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/v1.json"
        }
    with pytest.raises(NotImplementedError) as err:
        get_pipeline_identifier_from_config(config_dict)
        
    assert str(err.value) == "Current acceptable formats require 'pipeline' to be in the top level of the config dict"


