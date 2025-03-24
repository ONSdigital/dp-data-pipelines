from pathlib import Path
from unittest.mock import MagicMock

    
class MockLocalDirectoryStore:
    calls = []
    def __init__(self, local_dir: Path, expected_path: str, files_to_return: list[str]):
        self.local_dir = local_dir
        self.expected_path = expected_path
        self.files_to_return = files_to_return
        
    def get_file_names(self):
        str_local_dir = str(self.local_dir)
        self.calls.append(str_local_dir)
        if str_local_dir == self.expected_path:
            return self.files_to_return
        
        return []
    
def mock_path_constructor(path_arg: str, r_glob_return_value: list, path_instances: dict):
    mock_instance = MagicMock(name=f"Path({path_arg})")
    path_instances[path_arg] = mock_instance
    
    mock_instance.rglob.return_value = r_glob_return_value
    
    return mock_instance

def mock_decompress_zip_file(zip_path: str, expected_path: str):
    if zip_path != expected_path:
        raise Exception(f"Expected {expected_path} but received {zip_path}")
    
    path = Path(zip_path)
    return path.stem