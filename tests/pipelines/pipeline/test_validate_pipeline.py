from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dpypelines.pipeline.metadata.metadata_models import (
    Manifest,
)
from dpypelines.pipeline.validate_pipeline import (
    validate_manifest,
)

test_cases_base_dir = Path("tests/fixtures/test-cases")


@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_validate_manifest(mock_local_store):
    mock_local_store = MagicMock(name="local_store")
    mock_local_store.get_lone_matching_json_as_dict.return_value = {
        "metadata_file": "metadata.json",
        "submission_contacts": [{"email": "jane.doe@ons.gov.uk"}],
        "use_previous_metadata": True,
    }
    manifest = validate_manifest(mock_local_store)

    assert isinstance(manifest, Manifest)
    assert manifest.metadata_file == "metadata.json"
    assert manifest.submission_contacts[0].email == "jane.doe@ons.gov.uk"


@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_validate_manifest_file_not_found(mock_local_store):
    mock_local_store = MagicMock(name="local_store")
    mock_local_store.get_lone_matching_json_as_dict.return_value = {}
    with pytest.raises(FileNotFoundError) as e:
        validate_manifest(mock_local_store)
    assert "Failed to retrieve manifest from the local directory store." in str(e)
