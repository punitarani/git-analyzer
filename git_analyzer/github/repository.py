"""
GitHub Repository Class File
"""

import asyncio

import aiohttp
import pandas as pd

from .github import GitHub

# Set asyncio event loop policy to WindowsSelectorEventLoopPolicy
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# Avoids "RuntimeError: Event loop is already running"


class Repository(GitHub):
    """GitHub Repository"""

    def __init__(self, owner: str, name: str):
        """
        Constructor

        Parameters
        ----------
        owner : str
            Owner of the repository
        name : str
            Name of the repository
        """

        # Call the superclass constructor
        super().__init__()

        # Params
        self.owner: str = owner
        self.name: str = name

        # GitHub API Caches
        self.ratelimit_limit: int = -1
        self.ratelimit_remaining: int = -1

    @property
    def url(self) -> str:
        """Get the URL of the repository"""

        return f"https://api.github.com/repos/{self.owner}/{self.name}"

    @property
    def exists(self) -> bool:
        """Check if Repository exists"""

        # Get the repository
        resp = self.get(
            self.url,
            headers=self.headers,
        )

        # Repository exists
        if resp.status_code == 200:
            return True

        # Repository does not exist
        return False

    def get_commits(self, max_pages: int = 1) -> pd.DataFrame:
        """
        Get the commits of the repository

        Parameters
        ----------
        max_pages : int
            Max number of num_pages to get. Default is 1

        Returns
        -------
        df : pd.DataFrame
            Table of Commits

        Notes
        -----
        Does not end at first commit.
        If max_pages > # of commits /100, API calls are wasted getting empty pages.
        However, this will not break the program.
        """

        # Base URL
        url = self.url + "/commits"

        # Build the URL
        url = url.format(
            owner=self.owner,
            name=self.name,
        )

        # Get the first page
        commits = self.__get_commits(url, page=1)

        # Get the other num_pages
        for page in range(2, max_pages + 1):
            commits.extend(self.__get_commits(url, page=page))

        # Convert to DataFrame
        # pylint: disable=invalid-name
        df = self.__convert_commits_to_df(commits)

        # Return the commits
        return df

    async def get_commits_async(
            self,
            max_pages: int = 1,
            session: aiohttp.ClientSession | None = None
    ) -> pd.DataFrame:
        """
        Get the commits of the repository asynchronously

        Parameters
        ----------
        max_pages : int
            Max number of num_pages to get. Default is 1
        session : aiohttp.ClientSession | None
            Session to use. Default is None (new session)

        Returns
        -------
        df : pd.DataFrame
            Table of Commits

        Notes
        -----
        Does not end at first commit.
        If max_pages > # of commits /100, API calls are wasted getting empty pages.
        However, this will not break the program.
        """

        __session_given: bool = session is not None

        # Base URL
        url = self.url + "/commits"

        # Build the URL
        url = url.format(
            owner=self.owner,
            name=self.name,
        )

        # TODO: Check problems with not using context manager
        # Create a session if one is not provided
        if session is None:
            session = aiohttp.ClientSession()

        # Create a set to store the tasks
        tasks = set()

        # Create tasks
        for page in range(1, max_pages + 1):
            task = self.__get_commits_async(session=session, url=url, page=page)

            # Add the task to the set
            tasks.add(task)

        # Gather the tasks
        commits = await asyncio.gather(*tasks, return_exceptions=False)

        # Wait for the tasks to finish
        await asyncio.sleep(0)

        # Close the session if open
        if not __session_given and session.closed is False:
            await session.close()

        # Unpack the nested list of commits
        commits = [item for sublist in commits for item in sublist]

        # Convert to DataFrame
        # pylint: disable=invalid-name
        df = self.__convert_commits_to_df(commits)

        # Return the commits
        return df

    def __get_commits(self, url: str, page: int = 1) -> list:
        """
        Get the commits

        Parameters
        ----------
        url : str
            URL to get the commits
        page : int
            Page to get the commits

        Returns
        -------
        commits : list
            List of commits
        """

        # Build the params
        params = {
            "page": page,
            "per_page": "100",
        }

        # Get the commits
        commits = self.get(url, headers=self.headers, params=params).json()

        # Return the commits
        return commits

    async def __get_commits_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        page: int = 1,
    ) -> list:
        """
        Get the commits

        :param url: URL to get the commits
        :return: List of commits
        """

        # Build the params
        params = {
            "page": page,
            "per_page": "100",
        }

        # Get the commits with get_async
        resp = await self.get_async(
            url=url, headers=self.headers, params=params, session=session
        )

        # Get the commits from the response
        commits = await resp.json()

        return commits

    @staticmethod
    def __convert_commits_to_df(commits: list) -> pd.DataFrame:
        """
        Convert the commits to a dataframe

        Parameters
        ----------
        commits : list
            List of commits

        Returns
        -------
        df : pd.DataFrame
            Table of Commits
        """

        # Convert to dataframe
        # pylint: disable=invalid-name
        df = pd.DataFrame(commits)

        # Rename index column 'index'
        df.index.name = "index"

        # Return the dataframe
        return df


class RepositoryNotFoundError(Exception):
    """
    Exception raised when the repository is not found
    """

    def __init__(self, url: str):
        message: str = f"Repository not found: {url}"
        super().__init__(message)
