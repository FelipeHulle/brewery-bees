#%%
import requests
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count
from typing import Optional, Dict, Any

class APIClient:
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 10) -> None:
        """
        Initializes the API client.

        :param base_url: Base URL of the API.
        :param headers: Dictionary with default headers for all requests.
        :param timeout: Timeout for requests, in seconds.
        """
        self.base_url = base_url
        self.headers = headers or {}
        self.timeout = timeout

    def get(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Any:
        """
        Sends a GET request.

        :param endpoint: API endpoint.
        :param params: Dictionary of query string parameters.
        :return: API response.
        """
        url = f"{self.base_url}{endpoint}"
        response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        return self._handle_response(response)

    def post(self, endpoint: str, data: Optional[Any] = None, json: Optional[Any] = None) -> Any:
        """
        Sends a POST request.

        :param endpoint: API endpoint.
        :param data: Data to send in the request body (standard format).
        :param json: JSON data to send in the request body.
        :return: API response.
        """
        url = f"{self.base_url}{endpoint}"
        response = requests.post(url, headers=self.headers, data=data, json=json, timeout=self.timeout)
        return self._handle_response(response)

    def put(self, endpoint: str, data: Optional[Any] = None, json: Optional[Any] = None) -> Any:
        """
        Sends a PUT request.

        :param endpoint: API endpoint.
        :param data: Data to send in the request body (standard format).
        :param json: JSON data to send in the request body.
        :return: API response.
        """
        url = f"{self.base_url}{endpoint}"
        response = requests.put(url, headers=self.headers, data=data, json=json, timeout=self.timeout)
        return self._handle_response(response)

    def delete(self, endpoint: str) -> Any:
        """
        Sends a DELETE request.

        :param endpoint: API endpoint.
        :return: API response.
        """
        url = f"{self.base_url}{endpoint}"
        response = requests.delete(url, headers=self.headers, timeout=self.timeout)
        return self._handle_response(response)

    def _handle_response(self, response: requests.Response) -> Any:
        """
        Handles the API response, checking for errors and returning data.

        :param response: API response object.
        :return: Response content if successful.
        """
        try:
            response.raise_for_status()  # Raises an error if the status is not 2xx
        except requests.exceptions.HTTPError as e:
            return {"error": str(e), "status_code": response.status_code}
        try:
            return response.json()  # Tries to return the response as JSON
        except ValueError:
            return response.text  # Returns as text if not JSON

