import json

def get_transform_identifier_from_config(config: dict) -> str:
    """
    Given a pipeline config as a dictionary, return the identifier
    declaring the transform logic to be used
    """

    if config["$id"].endswith("/schemas/dataset-ingress/config/v1.json"):
        # Expects
        # "options": {
        #   "transform_identifier": <HERE>
        # }

        assert "options" in config.keys(), (
            "Config dict does not have expected root level options key",
            f"Got config of: {json.dumps(config, indent=2)}"
        )
        assert "transform_identifier" in config["options"].keys(), (
            "Config dict does not have expected 'transform_identifier' field as a child of 'options'. "
            f"Got config of: {json.dumps(config, indent=2)}"
        ) 
        identifier = config["options"]["transform_identifier"]
        return identifier

    else:
        # can add more schemas to the condition
        # use an 'elif' statement the same as the first to include the new name (v2 for example)
        # of the schema
        raise NotImplementedError(f"{config['$id']} is not a recognised $id")
