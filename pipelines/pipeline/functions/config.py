def get_pipeline_identifier_from_config(config: dict) -> str:
    """
    Given a pipeline config as a dictionary, return the identifier
    identifier is the contents of the pipeline field
    """

    if config["$id"].endswith("/schemas/dataset-ingress/config/v1.json"):
        try:
            identifier = config["pipeline"]
            return identifier
        
        except Exception as e:
            raise NotImplementedError("Current acceptable formats require 'pipeline' to be in the top level of the config dict")
            
    else:
        # can add more schemas to the condition
        # either use an 'elif' or amend the first if statement to include the new name (v2)
        # of the schema, providing 'pipeline' is a key
        raise NotImplementedError("Only currently built to accept '/schemas/dataset-ingress/config/v1.json' as valid $id")
    