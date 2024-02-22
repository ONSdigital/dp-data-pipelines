from pathlib import Path


def get_config_schema_path(config: dict) -> Path:
    """
    Returns the local path to a schema from the `$id` specified in a pipeline config dictionary
    """
    # Set the base path for the schemas
    schema_base_path = Path("schemas/dataset-ingress/config").absolute()
    # Check `$id` is in the config dictionary keys
    if "$id" not in config.keys():
        raise KeyError("No `$id` field in config")
    config_id_path = Path(config["$id"])
    # Get the config schema version used
    config_schema_version = config_id_path.name
    # Get the local path to the schema and check it exists
    local_schema_path = schema_base_path / config_schema_version
    if not local_schema_path.exists():
        raise FileNotFoundError(f"Local schema not found at {str(local_schema_path)}")
    return local_schema_path
