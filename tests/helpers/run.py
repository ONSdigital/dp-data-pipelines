from tests.helpers.api_utils import get_random_dataset_with_edition
from tests.helpers.file_utils import create_successful_file
from tests.helpers.upload_file_to_s3 import upload_and_test

save_to_disk = True

dataset_to_use = get_random_dataset_with_edition()
edition_id = (
    dataset_to_use.edition.current.edition
    if dataset_to_use.edition.current is not None
    else dataset_to_use.edition.next.edition
)
file = create_successful_file(
    dataset_id=dataset_to_use.dataset.id,
    edition_id=edition_id,
    save_to_disk=save_to_disk,
)
upload_and_test(file.zip_file, file.file_name)
