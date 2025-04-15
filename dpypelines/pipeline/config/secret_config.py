from typing import List, Optional

from dpypelines.pipeline.config.secret_mapping import SecretMapping


class SecretConfig:
    def __init__(
        self,
        secret_id: str,
        config_attribute: Optional[str] = None,
        mappings: Optional[List[SecretMapping]] = None,
    ):  # noqa: F821
        """
        Configuration for _a single AWS Secret Manager_ and how it maps to the JobConfiguration class

        :param secret_id: The ID/name of the secret in AWS Secrets Manager
        :param config_attribute: The attribute in JobConfiguration to store the raw secret value
        :param mappings: List of tuples mappings for a JSON secret
        """
        self.secret_id = secret_id
        self.config_attribute = config_attribute
        self.mappings = mappings
