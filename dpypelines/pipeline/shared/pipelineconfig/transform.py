import json
from typing import Callable, Dict, Any

def get_transform_details(config: Dict, transform_field: str) -> Any:
    """
    """
    if config["config_version"] == 1:
        assert (
            transform_field in config.keys()
        ), f"""'{transform_field}' not found in config dictionary:
        {json.dumps(config, indent=2, default=lambda x: str(x))}"""
        transform_field_to_get = config[transform_field]
        return transform_field_to_get
    else:
        raise NotImplementedError(
            f"Config version {config['config_version']} not recognised"
        )