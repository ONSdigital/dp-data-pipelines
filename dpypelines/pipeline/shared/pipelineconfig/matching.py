"""This script will contain functions for the pipeline"""

from typing import List


def get_supplementary_distribution_patterns(config: dict) -> List[str]:
    """
    Given a pipeline config in the form of a dictionary, return
    a list of regex patterns for finding supplementary distributions.

    Raise an exception where the pipeline config is not one we have
    handling for.
    """
    list_of_matches = []
    if config["$id"].endswith("initial-structure/schemas/ingress/config/v1.json"):
        for x in config["supplementary_distributions"]:
            value = x["matches"]
            list_of_matches.append(value)
        return list_of_matches
    else:
        raise ValueError(
            f"No supplementary distribution handling for config {config['$id']}"
        )
