from dpypelines.pipeline.configuration import get_pipeline_config


def test_get_pipeline_config():
    config = {"^cpih$": {"config_version": 1}, "^.*$": {"config_version": 2}}
    pipeline_config = get_pipeline_config("cpih", config)
    assert pipeline_config["config_version"] == 1


def test_get_pipeline_config_default():
    config = {"^cpih$": {"config_version": 1}, "^.*$": {"config_version": 2}}
    pipeline_config = get_pipeline_config("some-id", config)
    assert pipeline_config["config_version"] == 2
