from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.etl_processor import ETLProcessor

logger = DpLogger("data-ingress-pipeline")


def start(s3_object_name: str, *args, **kwargs):
    """
    Handles the required behaviour when receiving a zip file indicated by an S3 object name.
    Allows extra arguments to be passed (if used as a secondary function in the pipeline config).

    Args:
        s3_object_name (str): The S3 object name of the zip file to be processed.
        *args: Optional extra positional arguments.
        **kwargs: Optional extra keyword arguments.
    """
    # Create ETL processor object.
    etl_processor = ETLProcessor(s3_object_name=s3_object_name)

    # Check if dataset exists in datasets collection and create if not exists, with new status
    dataset, status_oid = (
        etl_processor.db_datasets_service.create_dataset_if_not_exists(
            s3_object=etl_processor.s3_object
        )
    )

    # Process S3 object
    s3_object_processed = etl_processor.process_s3_object_event(status_oid)
    return s3_object_processed
