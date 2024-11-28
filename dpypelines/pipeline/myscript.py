from dpypelines.pipeline.configuration import get_pipeline_config
from dpypelines.pipeline.generic_file_ingress_v1 import generic_file_ingress_v1

# `files_dir` should be a string of the directory containing your data source(s)
files_dir = "/Users/muazzamchaudhary/git/dp-data-pipelines/dpypelines/pipeline/input/"

# Specify the source id for accessing pipeline configuration details
pipeline_config = get_pipeline_config("_move")

# Run the function
generic_file_ingress_v1(files_dir, pipeline_config)