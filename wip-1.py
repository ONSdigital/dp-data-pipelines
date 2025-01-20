from dotenv import load_dotenv
import os
from dpytools.http.dataset.dataset_api_client import DatasetAPIClient
from dpytools.stores.directory.local import LocalDirectoryStore

load_dotenv()


# Get id of metadata:

# hardcode for now: 
dataset_id = "asdfasdfasdf"
url_path = f"datasets/{dataset_id}"
url_netloc = os.environ.get("URL_NETLOCK", None)

local_store = LocalDirectoryStore("tests/test_data/valid_metadata")
files_in_directory = local_store.get_file_names()
print('files_in_directory', files_in_directory)

def check_dataset_id_exists(dataset_id: str) -> bool:
    dataset_api = DatasetAPIClient(url_netloc, url_path)
    try:
        res = dataset_api.get_path()
    except Exception as e:
        res = e.response

    print('RES', res)
    return res.status_code == 404

print('DOES ID EXIST', check_dataset_id_exists(dataset_id))


