"""This script will contain functions for the pipeline"""
from multiprocessing.sharedctypes import Value
from typing import List


def get_supplementary_distribution_patterns(config: dict) -> List[str]:
    """
    Given a pipeline config in the form of a dictionary, return
    a list of regex patterns for finding supplementary distributions.
    """
    expected_id_value = "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"

    list_of_matches = []
    if config["$id"] != expected_id_value:
        raise ValueError(f"the `$id` value is missing or incorrect! It should be {expected_id_value}")
    else:
        for x in config["supplementary_distributions"]:
            value = x["matches"]
            list_of_matches.append(value)

        return list_of_matches

def get_required_file_patterns(config: dict) -> List[str]:
    """
    Given a pipeline config in the form of a dictionary, return
    a list of regex patterns for finding supplementary distributions.
    """
    expected_id_value = "https://raw.githubusercontent.com/ONSdigital/sandbox/initial-structure/schemas/ingress/config/v1.json"

    list_of_matches = []
    if config["$id"] != expected_id_value:
        raise ValueError(f"the `$id` value is missing or incorrect! It should be {expected_id_value}")
    else:
        for x in config["required_files"]:
            value = x["matches"]
            list_of_matches.append(value)

        return list_of_matches