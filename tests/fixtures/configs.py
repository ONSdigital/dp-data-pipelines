import json
from pathlib import Path

import pytest

this_dir = Path(__file__).parent
test_cases_dir = Path(this_dir / "test-cases")


@pytest.fixture
def config_valid_id():
    with open(test_cases_dir / "test_pipeline_config_valid_id.json") as f:
        config = json.load(f)
    return config


@pytest.fixture
def config_invalid_id():
    with open(test_cases_dir / "test_pipeline_config_invalid_id.json") as f:
        config = json.load(f)
    return config


@pytest.fixture
def config_valid_multiple_matches():
    with open(test_cases_dir / "test_pipeline_config_multiple_matches.json") as f:
        config = json.load(f)
    return config


@pytest.fixture
def config_no_required_files():
    with open(test_cases_dir / "test_pipeline_config_no_required_files.json") as f:
        config = json.load(f)
    return config


@pytest.fixture
def config_no_supplementary_distributions():
    with open(
        test_cases_dir / "test_pipeline_config_no_supplementary_distributions.json"
    ) as f:
        config = json.load(f)
    return config
