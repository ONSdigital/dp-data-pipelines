import json
from typing import List


def get_matching_pattern(config: dict, pattern: str) -> List[str]:
    """ """
    if config["config_version"] == 1:
        assert (
            pattern in config.keys()
        ), f"""'{pattern}' field not found in config dictionary:
    {json.dumps(config, indent=2, default=lambda x: str(x))}"""
        matches = [pattern["matches"] for pattern in config[pattern]]
        return matches
    else:
        raise NotImplementedError(
            f"Config version {config['config_version']} not recognised"
        )
