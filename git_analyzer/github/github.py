""" GitHub Base Class File """

import asyncio
import os
import sys

import aiohttp
import requests


# Set asyncio event loop policy to WindowsSelectorEventLoopPolicy
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# Avoids "RuntimeError: Event loop is already running"


class GitHub:
    """GitHub API Base Class"""

    # Get gh_token from environment
    token = os.environ.get("GH_TOKEN")

    def __init__(self):
        """Constructor"""

        # Generate the headers for requests
        self.headers: dict = self.build_headers()

        # GitHub API Ratelimit info
        self.ratelimit_limit: int = -1
        self.ratelimit_remaining: int = -1

        # Cache last response
        self.response: requests.Response | aiohttp.ClientResponse | None = None

    def build_headers(self) -> dict:
        """Build the headers for requests"""

        # Build the headers
        headers = {
            "Accept": "application/vnd.github+json",
            "per_page": "100",
            "page": "1",
        }

        # Add token if available
        if self.token:
            headers["Authorization"] = f"token {self.token}"

        # Cache the headers if not already cached
        if getattr(self, "headers", None) is None:
            self.headers = headers

        return headers

    def get(
        self,
        url: str,
        headers: dict = None,
        params: dict = None,
    ) -> requests.Response:
        """
        Get the data from the URL

        Parameters
        ----------
        url : str
            Endpoint URL
        headers : dict, optional
            Headers to send with the request. Default is self.headers.
        params : dict, optional
            Parameters to send with the request

        Returns
        -------
        requests.Response
            Response from the request
        """

        # Build the headers
        if headers is None:
            headers = self.headers

        # Get the data
        if params is None:
            resp = requests.get(url, headers=headers)
        # Add params if available
        else:
            resp = requests.get(url, headers=headers, params=params)

        # Get the ratelimit info from the headers
        if "x-ratelimit-limit" in resp.headers:
            self.ratelimit_limit = int(resp.headers["x-ratelimit-limit"])
        if "x-ratelimit-remaining" in resp.headers:
            self.ratelimit_remaining = int(resp.headers["x-ratelimit-remaining"])

        # Cache the response
        self.response = resp

        # Return the data
        return resp

    async def get_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict = None,
        params: dict = None,
    ) -> aiohttp.ClientResponse:
        """
        Async get() method to retrieve data from the URL

        Parameters
        ----------
        session : aiohttp.ClientSession
            Session to use for the request
        url : str
            Endpoint URL
        headers : dict, optional
            Headers to send with the request. Default is self.headers.
        params : dict, optional
            Parameters to send with the request

        Returns
        -------
        aiohttp.ClientResponse
            Response from the request
        """

        # Build the headers
        if headers is None:
            headers = self.headers

        # Get the data
        if params is None:
            resp = await session.get(url, headers=headers)
        # Add params if available
        else:
            resp = await session.get(
                url,
                headers=headers,
                params=params,
            )

        # Get the ratelimit info from the headers
        if "x-ratelimit-limit" in resp.headers:
            self.ratelimit_limit = int(resp.headers["x-ratelimit-limit"])
        if "x-ratelimit-remaining" in resp.headers:
            self.ratelimit_remaining = int(resp.headers["x-ratelimit-remaining"])

        # Cache the response
        self.response = resp

        return resp
