from dpypelines.pipeline.configuration import get_pipeline_config


def test_get_pipeline_config():
    pipeline_config, config_keys = get_pipeline_config("cpih")
    assert pipeline_config["config_version"] == 1


def test_get_pipeline_config_default():
    pipeline_config, config_keys = get_pipeline_config("some-id")
    assert pipeline_config["config_version"] == 1
