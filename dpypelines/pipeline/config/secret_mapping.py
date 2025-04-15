class SecretMapping:
    def __init__(self, secret_key: str, config_attribute: str):
        """
        Initialize a new secret mapping

        :param secret_key: The key in the secret value dictionary
        :param config_attribute: The attribute name in JobConfiguration to set
        """
        self.secret_key = secret_key
        self.config_attribute = config_attribute
