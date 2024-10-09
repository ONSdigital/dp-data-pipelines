#from dpypelines import s3_tar_received

#s3_tar_received.start('joes-bucket-will-be-deleted/config-no-options.tar')

import sys
from behave.__main__ import run_behave
from behave.configuration import Configuration

if __name__ == "__main__":
    # args = sys.argv[1:] if len(sys.argv) > 1 else []
    args = [
        "--verbose",
        "features/dataset_ingress.feature",  # Feature file path
        "-n",
        "Generic ingress runs without errors",  # Scenario text
    ]
    configuration = Configuration(args)
    sys.exit(run_behave(configuration))