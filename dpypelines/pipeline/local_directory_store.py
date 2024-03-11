import json
import os
from dpypelines.pipeline.functions.config import get_pipeline_identifier_from_config
from dpypelines.pipeline.shared.pipelineconfig.matching import get_supplementary_distribution_patterns , get_required_files_patterns


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

    def verify_required_file(self):
        """
        Verify that the specified required files have been provided.
        """
        required_files = get_required_files_patterns(self.get_pipeline_config())
        for file in required_files:
            if not os.path.isfile(os.path.join(self.directory, file)):
                return False
        return True

    def  verify_supplementary_distribution(self):
        """
        Verify that the specified supplementary distributions have been provided.
        """
        supplementary_distributions = get_supplementary_distribution_patterns(self.get_pipeline_config())
        for file in supplementary_distributions:
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

        return get_pipeline_identifier_from_config(config)