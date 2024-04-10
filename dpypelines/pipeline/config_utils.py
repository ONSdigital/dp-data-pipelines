import re


def get_dataset_id(s3_object_name: str) -> str:
    """
    Placeholder function to be updated once we know where the dataset_id can be extracted from (not necessarily s3_object_name as suggested by argument name)
    """
    return "not-specified"


def get_pipeline_config(dataset_id: str, configuration: dict) -> dict:
    """
    Get pipeline config details for the given dataset_id
    """
    for key in configuration.keys():
        if re.match(key, dataset_id):
            pipeline_config = configuration[key]
            break
    return pipeline_config
