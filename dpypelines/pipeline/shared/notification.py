import os

from dpytools.slack.slack import SlackMessenger

PS_SLACK_WEBHOOK = os.environ.get("PS_SLACK_WEBHOOK", None)
assert (
    PS_SLACK_WEBHOOK is not None
), "Unable to find required environment variable PS_SLACK_WEBHOOK"
ps_slack_client = SlackMessenger(PS_SLACK_WEBHOOK)


def publishing_support(msg: str):
    """
    Send a messge to a channel or avanue of contact such that it reaches the
    publishing support team.
    """
    ps_slack_client.msg_str(msg)


DE_SLACK_WEBHOOK = os.environ.get("DE_SLACK_WEBHOOK", None)
assert (
    DE_SLACK_WEBHOOK is not None
), "Unable to find required environment variable DE_SLACK_WEBHOOK"
de_slack_client = SlackMessenger(DE_SLACK_WEBHOOK)


def data_engineering(msg: str):
    """
    Send a messge to a channel or avanue of contact such that it reaches the
    data engineers.
    """
    de_slack_client.msg_str(msg)


# IMPORTANT - where we're notifying the SEs we also want to notify the DEs so they
# know there's a software issues that's being looked into.
SE_SLACK_WEBHOOK = os.environ.get("SE_SLACK_WEBHOOK", None)
assert (
    SE_SLACK_WEBHOOK is not None
), "Unable to find required environment variable SE_SLACK_WEBHOOK"
se_slack_client = SlackMessenger(SE_SLACK_WEBHOOK)


def software_engineering(msg: str):
    """
    Send a messge to a channel or avaenue of contact such that it reaches the
    software engineers.
    """
    se_slack_client.msg_str(msg)


# IMPORTANT - where we're notifying the submitter we also want to notify the DEs so they
# know there's a submission issue that's being looked into.
DS_SLACK_WEBHOOK = os.environ.get("DS_SLACK_WEBHOOK", None)
assert (
    DS_SLACK_WEBHOOK is not None
), "Unable to find required environment variable DS_SLACK_WEBHOOK"
ds_slack_client = SlackMessenger(DS_SLACK_WEBHOOK)


def data_submmitter_of_validation_error(msg: str):
    """
    Send a messge to a channel or avaenue of contact such that it reaches the
    registred contact email the data was submitted with. Include information on
    a specific json schema validation error.
    """
    ds_slack_client.msg_str(msg)
