import json
import os
#from dpytools.http_clients.base import BaseHttpClient


from pipeline.shared.functons.slack import SlackNotifier

#slack_webhook_env_var = os.environ["SLACK_WEBHOOK"]

def publishing_support(publishing_webhook_url: str, msg: str):
    """
    Send a messge to a channel or avanue of contact such that it reaches the
    publishing support team.
    """
    publishing_notifier = SlackNotifier(publishing_webhook_url)
    publishing_notifier.msg_str(msg)

def data_engineering(data_eng_webhook_url: str, msg: str):
    """
    Send a messge to a channel or avanue of contact such that it reaches the
    data engineers.
    """
    data_engineering_notifier = SlackNotifier(data_eng_webhook_url)
    data_engineering_notifier.msg_str(msg)

# IMPORTANT - where we're notifying the SEs we also want to notify the DEs so they
# know there's a software issues that's being looked into.
def software_engineering(software_eng_webhook_url, msg):
    """
    Send a messge to a channel or avaenue of contact such that it reaches the
    software engineers.
    """
    #software engineer slack notifier
    #software engineer message code
    software_engineering_notifier = SlackNotifier(software_eng_webhook_url)
    software_engineering_notifier.msg_str(msg)

# IMPORTANT - where we're notifying the submitter we also want to notify the DEs so they
# know there's a submission issue that's being looked into.
def data_submmitter_of_validation_error(data_submitter_webhook_url, msg):
    """
    Send a messge to a channel or avaenue of contact such that it reaches the
    registred contact email the data was submitted with. Include information on
    a specific json schema validation error.
    """
    data_submmitter_of_validation_error_notifier = SlackNotifier(data_submitter_webhook_url)
    data_submmitter_of_validation_error_notifier.msg_str(msg)