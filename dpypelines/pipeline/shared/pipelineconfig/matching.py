import json
from typing import List


def get_supplementary_distribution_patterns(config: dict) -> List[str]:
    """
    Given a pipeline config in the form of a dictionary, return
    a list of regex patterns for finding supplementary distributions.
    """
    if config["config_version"] == 1:
        assert (
            "supplementary_distributions" in config.keys()
        ), f"""'supplementary_distributions' field not found in config dictionary:
    {json.dumps(config, indent=2, default=lambda x: str(x))}"""
        matches = [
            supplementary_distribution["matches"]
            for supplementary_distribution in config["supplementary_distributions"]
        ]
        return matches
    else:
        raise NotImplementedError(
            f"Config version {config['config_version']} not recognised"
        )


def get_required_files_patterns(config: dict) -> List[str]:
    """
    Given a pipeline config in the form of a dictionary, return
    a list of regex patterns for finding required files.
    """
    if config["config_version"] == 1:
        assert (
            "required_files" in config.keys()
        ), f"""'required_files' field not found in config dictionary:
    {json.dumps(config, indent=2, default=lambda x: str(x))}"""
        matches = [
            required_files["matches"] for required_files in config["required_files"]
        ]
        return matches
    else:
        raise NotImplementedError(
            f"Config version {config['config_version']} not recognised"
        )
