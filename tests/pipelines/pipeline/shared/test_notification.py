import pytest

def test_notify_se_webook_raises_for_missing_env_var():
    """
    Test that when we import notify, an assertion error
    is correctly raised if we're missing the env var
    specifying the software engineering slack webhook
    """

    # use monkeypatch to set all the env vars other than
    # the one you're testing for the absense of
    mp = pytest.MonkeyPatch()
    mp.setenv("PS_SLACK_WEBHOOK", "not-used")
    mp.setenv("DE_SLACK_WEBHOOK", "not-used")
    mp.setenv("DS_SLACK_WEBHOOK", "not-used")

    with pytest.raises(AssertionError) as e:
        from pipelines.pipeline.shared import notification

    assert "Unable to find required environment variable SE_SLACK_WEBHOOK" in str(e)

def test_notify_ps_webook_raises_for_missing_env_var():
    """
    Test that when we import notify, an assertion error
    is correctly raised if we're missing the env var
    specifying the software engineering slack webhook
    """

    # use monkeypatch to set all the env vars other than
    # the one you're testing for the absense of
    mp = pytest.MonkeyPatch()
    mp.setenv("SE_SLACK_WEBHOOK", "not-used")
    mp.setenv("DE_SLACK_WEBHOOK", "not-used")
    mp.setenv("DS_SLACK_WEBHOOK", "not-used")

    with pytest.raises(AssertionError) as e:
        from pipelines.pipeline.shared import notification

    assert "Unable to find required environment variable PS_SLACK_WEBHOOK" in str(e)  

def test_notify_de_webook_raises_for_missing_env_var():
    """
    Test that when we import notify, an assertion error
    is correctly raised if we're missing the env var
    specifying the software engineering slack webhook
    """

    # use monkeypatch to set all the env vars other than
    # the one you're testing for the absense of
    mp = pytest.MonkeyPatch()
    mp.setenv("PS_SLACK_WEBHOOK", "not-used")
    mp.setenv("SE_SLACK_WEBHOOK", "not-used")
    mp.setenv("DS_SLACK_WEBHOOK", "not-used")

    with pytest.raises(AssertionError) as e:
        from pipelines.pipeline.shared import notification

    assert "Unable to find required environment variable DE_SLACK_WEBHOOK" in str(e) 

def test_notify_ds_webook_raises_for_missing_env_var():
    """
    Test that when we import notify, an assertion error
    is correctly raised if we're missing the env var
    specifying the software engineering slack webhook
    """

    # use monkeypatch to set all the env vars other than
    # the one you're testing for the absense of
    mp = pytest.MonkeyPatch()
    mp.setenv("PS_SLACK_WEBHOOK", "not-used")
    mp.setenv("DE_SLACK_WEBHOOK", "not-used")
    mp.setenv("SE_SLACK_WEBHOOK", "not-used")

    with pytest.raises(AssertionError) as e:
        from pipelines.pipeline.shared import notification

    assert "Unable to find required environment variable DS_SLACK_WEBHOOK" in str(e)         