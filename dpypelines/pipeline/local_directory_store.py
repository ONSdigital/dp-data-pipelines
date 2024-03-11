import json
import os

from dpypelines.pipeline.shared.pipelineconfig.matching import get_supplementary_distribution_patterns


class LocalDirectoryStore:
    """
    Class to represent a local directory store.
    """
    def __init__(self, directory_path: str):
        """
        directory: Path to the directory where the input
        files for this pipeline are located.
        """
        self.directory = directory_path

    def verify_required_file(self, required_file: list):
        """
        Verify that the specified required files have been provided.
        """
        for file in required_file:
            if not os.path.exists(os.path.join(self.directory, file)):
                return False
        return  True

    def  verify_supplementary_distribution(self):
        """
        Verify that the specified supplementary distributions have been provided.
        """
        for file in get_supplementary_distribution_patterns:
            if not os.path.isfile(os.path.join(self.directory, file)):
                return False
        return True
    
    def get_pipeline_config(self):
        """
        Get the pipeline configuration.
        """
        config_file = os.path.join(self.directory, 'pipeline-config.json')
        if not os.path.exists(config_file):
            return None

        with open(config_file, 'r') as f:
            config = json.load(f)

        return config.get('pipeline')