import pytest

from dpypelines.pipeline.utils import get_value_from_metadata


def test_get_value_exists_in_metadata():
    metadata = {
        "@id": "id",
        "@type": "dcat:DatasetSeries",
        "dcterms:identifier": "dataset_id",
        "dcterms:title": "dataset_title",
        "dcterms:description": "dataset_description",
    }
    value = get_value_from_metadata(metadata, "dcterms:identifier")
    assert value == "dataset_id"


def test_get_value_does_not_exist_in_metadata():
    metadata = {
        "@id": "id",
        "@type": "dcat:DatasetSeries",
        "dcterms:title": "dataset_title",
        "dcterms:description": "dataset_description",
    }
    with pytest.raises(KeyError) as e:
        get_value_from_metadata(metadata, "dcterms:identifier")

    assert "KeyError" in str(e)
