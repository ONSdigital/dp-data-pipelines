from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1

config =     {"^.*_move$": {
        "config_version": 1,
        "transform": None,
        "transform_inputs": {},
        "transform_kwargs": {},
        "required_files": [
            {"matches": "^data.csv$"},
            {"matches": "^metadata.json$"},
        ],
        "supplementary_distributions": {},
        "secondary_function": dataset_ingress_v1,
    }}


local_file_directoy = "data/"


dataset_ingress_v1(local_file_directoy, config)