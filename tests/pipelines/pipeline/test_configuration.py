from dpypelines.pipeline.configuration import get_pipeline_config


def test_get_pipeline_config():
    pipeline_config = get_pipeline_config("some-id_v2_0")
    assert pipeline_config["config_version"] == 1


def test_get_pipeline_config_default():
    pipeline_config = get_pipeline_config("some-id_v2_1")
    assert pipeline_config["config_version"] == 1
