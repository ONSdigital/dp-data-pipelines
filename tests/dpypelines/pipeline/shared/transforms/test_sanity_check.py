from pathlib import Path

import pytest

from dpypelines.pipeline.shared.transforms.sanity_check import sdmx_sanity_check_v1


def test_sdmx_sanity_check_v1_file_extension():
    with pytest.raises(AssertionError) as e:
        sdmx_sanity_check_v1(Path("data.csv"))
    assert "Invalid sdmx_file" in str(e.value)


def test_sdmx_sanity_check_v1_read_xml():
    with pytest.raises(Exception) as e:
        sdmx_sanity_check_v1(Path("data.xml"))
    assert "Failed to read in xml" in str(e.value)
