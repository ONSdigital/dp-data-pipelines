import logging

import backoff
import requests
from requests.exceptions import HTTPError


# Function to log retry attempts
def log_retry(details):
    logging.error(f"Request failed, retrying... Attempt #{details['tries']}")


class BaseHttpClient:
    # Initialize HttpClient with a backoff_max value
    def __init__(self, backoff_max=30):
        self.backoff_max = backoff_max

    # GET request method with exponential backoff
    @backoff.on_exception(backoff.expo, HTTPError, max_time=30, on_backoff=log_retry)
    def get(self, url, *args, **kwargs):
        """
        Sends a GET request to the specified URL with optional extra arguments.

        This method is a thin wrapper around `requests.get()`. Any additional arguments
        are passed directly to `requests.get()`. For more information on the available
        arguments, refer to the `requests.get()` documentation:
        https://docs.python-requests.org/en/latest/api/#requests.get

        Args:
            url (str): The URL to send the GET request to.
            *args: Optional positional arguments passed to `requests.get()`.
            **kwargs: Optional keyword arguments passed to `requests.get()`.

        Returns:
            Response: The Response object from `requests.get()`.
        Raises:
            HTTPError: If the request fails for a network-related reason.
        """
        return self._handle_request("GET", url, *args, **kwargs)

    # POST request method with exponential backoff
    @backoff.on_exception(
        backoff.expo,
        HTTPError,
        max_time=30,
        on_backoff=log_retry,
    )
    def post(self, url, *args, **kwargs):
        """
        Sends a POST request to the specified URL with optional extra arguments.

        This method is a thin wrapper around `requests.post()`. Any additional arguments
        are passed directly to `requests.post()`. For more information on the available
        arguments, refer to the `requests.post()` documentation:
        https://docs.python-requests.org/en/latest/api/#requests.post

        Args:
            url (str): The URL to send the POST request to.
            *args: Optional positional arguments passed to `requests.post()`.
            **kwargs: Optional keyword arguments passed to `requests.post()`.

        Returns:
            Response: The Response object from `requests.post()`.

        Raises:
            HTTPError: If the request fails for a network-related reason.
        """
        return self._handle_request("POST", url, *args, **kwargs)

    # Method to handle requests for GET and POST
    def _handle_request(self, method, url, *args, **kwargs):
        logging.info(f"Sending {method} request to {url}")
        try:
            response = requests.request(method, url, *args, **kwargs)
            response.raise_for_status()
            return response

        except HTTPError as http_err:
            logging.error(
                f"HTTP error occurred: {http_err} when sending a {method} to {url} with headers {kwargs.get('headers')}"
            )
            raise http_err
        except Exception as err:
            logging.error(
                f"Other error occurred: {err} when sending a {method} to {url} with headers {kwargs.get('headers')}"
            )
            raise err