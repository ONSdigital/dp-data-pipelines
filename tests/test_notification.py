import pytest
from unittest.mock import patch, MagicMock
from requests import HTTPError, Response
from base import BaseHttpClient
from pipeline.shared.functons.notification import publishing_support, data_engineering, software_engineering, data_submmitter_of_validation_error

#from dpytools.slack.slack import SlackNotifier


@patch.object(BaseHttpClient, 'post')
def test_msg_str_publishing_support(mock_post):
    """
    Tests to ensure a string message can be sent to a 
    webhook URL using a slack notifier made for the 
    pubishing support notifications channel.
    """
    webhook_url = 'http://example.com'
    mock_response = MagicMock(Response)
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    publishing_support(webhook_url, "Test message for pubishing support")

    mock_post.assert_called_once_with(webhook_url, json={'text': 'Test message for publishing support'})

@patch.object(BaseHttpClient, 'post')
def test_msg_str_data_engineering(mock_post):
    """
    Tests to ensure a string message can be sent to a 
    webhook URL using a slack notifier made for the 
    data engineering notifications channel.
    """
    webhook_url = 'http://example.com'
    mock_response = MagicMock(Response)
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    data_engineering(webhook_url, "Test message for data engineers")

    mock_post.assert_called_once_with(webhook_url, json={'text': 'Test message for data engineers'})

@patch.object(BaseHttpClient, 'post')
def test_msg_str_software_engineering(mock_post):
    """
    Tests to ensure a string message can be sent to a 
    webhook URL using a slack notifier made for the 
    software engineering notifications channel.
    """
    webhook_url = 'http://example.com'
    mock_response = MagicMock(Response)
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    software_engineering(webhook_url, "Test message for software engineering")

    mock_post.assert_called_once_with(webhook_url, json={'text': 'Test message for software engineering'})

@patch.object(BaseHttpClient, 'post')
def test_msg_str_data_submmitter_of_validation_error(mock_post):
    """
    Tests to ensure a string message can be sent to a 
    webhook URL using a slack notifier made for the 
    data submmitter of validation error notifications channel.
    """
    webhook_url = 'http://example.com'
    mock_response = MagicMock(Response)
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    data_submmitter_of_validation_error(webhook_url, "Test message for data submmitter of validation error")

    mock_post.assert_called_once_with(webhook_url, json={'text': 'Test message for data submmitter of validation error'})        