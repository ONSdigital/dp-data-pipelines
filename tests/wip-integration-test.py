from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1

from dotenv import load_dotenv 

load_dotenv()


config = {
        "config_version": 1,
        "transform": None,
        "transform_inputs": {},
        "transform_kwargs": {},
        "required_files": [
            {"matches": ".*data.csv$"},
            {"matches": ".*metadata.json$"},
        ],
        "supplementary_distributions": {}
    }


data_path = "data/"

resuolt = dataset_ingress_v1(data_path, config)