import json
import os
from pathlib import Path


def get_config_schema_path(config: dict) -> Path:
    """
    Returns the local path to a schema from the `$id` specified in a pipeline config dictionary
    """
    
    # Getting the absolute path to the 'dpypelines/' directory
    dpypelines_absolute_path = Path(__file__).parent.parent.parent.absolute()

    # Set the base path for the schemas
    schema_base_path = Path("schemas/dataset-ingress/config")

    # Check `$id` is in the config dictionary keys
    if "$id" not in config.keys():
        raise KeyError(
            f"""No `$id` field in config:
                {json.dumps(config, indent=2)}"""
        )

    config_id_path = Path(config["$id"])

    # Get the config schema version used
    config_schema_version = config_id_path.name

    # Get the local path to the schema and check it exists
    local_schema_path = dpypelines_absolute_path / schema_base_path / config_schema_version
    if not local_schema_path.exists():
        all_schema_paths = [
            os.path.join(schema_base_path, file)
            for file in os.listdir(schema_base_path)
        ]
        raise FileNotFoundError(
            f"""Local schema not found from $id '{config['$id']}'.

Schema path provided: '{local_schema_path}'.

Available schemas are {all_schema_paths}"""
        )
    return local_schema_path
