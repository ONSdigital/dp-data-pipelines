from dpypelines.s3_folder_received import start
from tests.e2e.e2e_test_base import E2ETestBase
from tests.helpers.generators.dataset.generator import GeneratedDataset


def test_e2e_test_success(test_base: E2ETestBase):
    dataset: GeneratedDataset = test_base.generate_test_dataset(True)
    file = test_base.generate_and_upload_s3_file(
        dataset_id=dataset.dataset.id, edition_id=dataset.edition_id
    )

    # TODO: don't need to do this.
    # Just:
    # - Upload file to S3
    # - Wait X seconds and check if the S3 object has moved 
    #     - If not, retry Y times. If still not found then failed
    #.    - If moved:
    #.        - Check it's in the right place (processed)
    #         - Then check dataset API, upload service, DB
    result = start(file)

    assert result is True