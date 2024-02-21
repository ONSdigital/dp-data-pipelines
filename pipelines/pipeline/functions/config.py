def get_pipeline_identifier_from_config(config: dict) -> str:
    """
    Given a pipeline config as a dictionary, return the identifier
    identifier is the contents of the pipeline field
    """
    
    if config["$id"].endswith("/schemas/dataset-ingress/config/v1.json"):
        assert "pipeline" in config.keys(), "pipeline is not in the config dict"
        identifier = config["pipeline"]
        return identifier
        
    else:
        # can add more schemas to the condition
        # use an 'elif' statement the same as the first to include the new name (v2 for example)
        # of the schema
        raise NotImplementedError(f"{config['$id']} is not a recognised $id")
    