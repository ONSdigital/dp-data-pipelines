import json
from pathlib import Path

import pytest

this_dir = Path(__file__).parent
text_cases_dir = Path(this_dir / "test-cases")


@pytest.fixture
def config_unknown_id():
    with open(text_cases_dir / "test-pipeline-config-unknown-id.json") as f:
        config = json.load(f)
    return config


@pytest.fixture
def config_valid_multiple_supplementary_distributions():
    with open(text_cases_dir / "test-pipeline-config-multiple-supplementary-distributions.json") as f:
        config = json.load(f)
    return config
