import json
from typing import Callable, Dict


def get_transform_inputs(config: dict) -> Dict[str, Callable]:
    if config["config_version"] == 1:
        assert (
            "transform_inputs" in config.keys()
        ), f"""'transform_inputs' not found in config dictionary:
        {json.dumps(config, indent=2, default=lambda x: str(x))}"""
        transform_inputs = config["transform_inputs"]
        return transform_inputs
    else:
        raise NotImplementedError(
            f"Config version {config['config_version']} not recognised"
        )


def get_transform_function(config: dict) -> Callable:
    if config["config_version"] == 1:
        assert (
            "transform" in config.keys()
        ), f"""'transform' not found in config dictionary:
        {json.dumps(config, indent=2, default=lambda x: str(x))}"""
        transform_function = config["transform"]
        return transform_function
    else:
        raise NotImplementedError(
            f"Config version {config['config_version']} not recognised"
        )


def get_transform_kwargs(config: dict) -> dict:
    if config["config_version"] == 1:
        assert (
            "transform_kwargs" in config.keys()
        ), f"""'transform_kwargs' not found in config dictionary:
        {json.dumps(config, indent=2, default=lambda x: str(x))}"""
        transform_kwargs = config["transform_kwargs"]
        return transform_kwargs
    else:
        raise NotImplementedError(
            f"Config version {config['config_version']} not recognised"
        )
