""" GitHub Commit Class File """

import json
from pathlib import Path

import aiohttp
import pandas as pd

from .github import GitHub


class Commit(GitHub):
    """Commit"""

    def __init__(
            self,
            url: str,
            sha: str = None,
            auto_get: bool = True
    ):
        """
        Constructor

        Parameters
        ----------
        url : str
            URL of the commit
        auto_get : bool
            Automatically get the commit data
        """

        # Call the superclass constructor
        super().__init__()

        self.url: str = url
        self._sha: str = sha

        # Cache the response data from GitHub API
        self.data: json = None

        # Get the commit data if auto_get is True
        if auto_get:
            self.get_commit(cache=True)

        # Cache parsed data
        self.changes: pd.DataFrame | None = None

        # Cache file saved path
        self.file_path: Path | None = None

    # region Main Methods

    def get_commit(self, cache: bool = True) -> json:
        """
        Get the commit data from GitHub API

        Parameters
        ----------
        cache : bool
            Cache the response data from GitHub API to 'data' attribute

        Returns
        -------
        json
            Commit data from GitHub API
        """

        # parameters
        params = {
            "per_page": 100,
        }

        # Get the data
        resp = self.get(
            self.url,
            headers=self.headers,
            params=params,
        )

        # Cache the data if not already cached
        if cache and self.data is None:
            self.data = resp.json()

        # Return the data
        return resp.json()

    def parse(self) -> pd.DataFrame:
        """
        Parse the commit and organize files into a dataframe

        Returns
        -------
        pd.DataFrame
            Dataframe of modified files

        Notes
        -----
            - Columns: sha, filename, status, additions, deletions, changes,
            blob_url, raw_url, patch
        """

        # Get the commit data
        if self.data is None:
            self.get_commit()

        # Get the files and convert to a dataframe
        files_df = self.__files_to_df(self.files)

        # Cache the dataframe
        self.changes = files_df

        return files_df

    def save(self, overwrite: bool = False) -> Path:
        """
        Save the changes dataframe to a parquet file

        Parameters
        ----------
        overwrite : bool
            Overwrite the file if it already exists

        Returns
        -------
        Path
            Path to the saved file
        """

        # Build filename
        file_name = self.sha + ".parquet"

        # Build file path
        project_root = Path(__file__).parent.parent.parent
        file_path = project_root.joinpath("data", self.repository, file_name)

        # Create the folder if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Cache the file path
        self.file_path = file_path

        # Save if it doesn't exist or if overwrite is True
        if not file_path.exists() or overwrite:
            # Get and Parse the commit data
            if self.changes is None:
                self.parse()

            # Get the changes dataframe
            # pylint: disable=invalid-name
            df = self.changes

            # Save the dataframe to a parquet file
            df.to_parquet(file_path)

        # Return the file path
        return file_path

    # endregion Main Methods

    # region Async Methods

    async def get_commit_async(
        self, session: aiohttp.ClientSession, cache: bool = True
    ) -> json:
        """
        Get the commit data from GitHub API

        Parameters
        ----------
        session : aiohttp.ClientSession
            Session to use for the request
        cache : bool
            Cache the response data from GitHub API to 'data' attribute

        Returns
        -------
        json
            Commit data from GitHub API
        """

        # parameters
        params = {
            "per_page": 100,
        }

        # Get the data
        resp = await self.get_async(
            session=session, url=self.url, headers=self.headers, params=params
        )

        # Get the json data
        data = await resp.json()

        # Cache the data if not already cached
        if cache and self.data is None:
            self.data = data

        # Return the data
        return data

    async def parse_async(self, session: aiohttp.ClientSession) -> pd.DataFrame:
        """
        Parse the commit and organize files into a dataframe

        Parameters
        ----------
        session : aiohttp.ClientSession
            Session to use for the request

        Returns
        -------
        pd.DataFrame
            Dataframe of modified files

        Notes
        -----
            - Columns: sha, filename, status, additions, deletions, changes,
            blob_url, raw_url, patch
        """

        # Get the commit data
        if self.data is None:
            await self.get_commit_async(session=session)

        # Get the files and convert to a dataframe
        files_df = self.__files_to_df(self.files)

        # Cache the dataframe
        self.changes = files_df

        return files_df

    async def save_async(
            self,
            session: aiohttp.ClientSession,
            overwrite: bool = False
    ) -> Path:
        """
        Save the changes dataframe to a parquet file

        Parameters
        ----------
        session : aiohttp.ClientSession
            Session to use for the request
        overwrite : bool
            Overwrite the file if it already exists

        Returns
        -------
        Path
            Path to the saved file
        """

        # Build filename
        file_name = self.sha + ".parquet"

        # Build file path
        project_root = Path(__file__).parent.parent.parent
        file_path = project_root.joinpath("data", self.repository, file_name)

        # Create the folder if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Cache the file path
        self.file_path = file_path

        # Save if it doesn't exist or if overwrite is True
        if not file_path.exists() or overwrite:
            # Get and Parse the commit data
            if self.changes is None:
                await self.parse_async(session=session)

            # Get the changes dataframe
            # pylint: disable=invalid-name
            df = self.changes

            # Save the data to a parquet file
            df.to_parquet(file_path)

        # Return the file path
        return file_path

    # endregion Async Methods

    # region Helper Methods

    def get_file_path(self) -> Path:
        """
        Get the file path

        Returns
        -------
        Path
            Path to the file

        Notes
        -----
        Requires the commit data to be  retrieved and parsed.
        """

        # Build filename
        file_name = self.sha + ".parquet"

        # Build file path
        project_root = Path(__file__).parent.parent.parent
        file_path = project_root.joinpath("data", self.repository, file_name)

        return file_path

    @staticmethod
    def __files_to_df(files: list[dict[str, str]]) -> pd.DataFrame:
        """
        Convert the files to a dataframe

        Parameters
        ----------
        files : list[dict[str, str]]
            List of files json data from GitHub API
        """

        # Convert the files to a dataframe
        # pylint: disable=invalid-name
        df = pd.DataFrame(files)

        # Return the dataframe
        return df

    # endregion Helper Methods

    # region Properties

    @property
    def sha(self) -> str:
        """Get the sha of the commit"""
        return self.data["sha"] if self.data is not None else self._sha

    @property
    def node_id(self) -> str:
        """Node ID of the commit"""
        return self.data["node_id"]

    @property
    def committer(self) -> dict[str, str]:
        """Committer Details (repo, email, date)"""
        return self.data["commit"]["committer"]

    @property
    def message(self) -> str:
        """Commit Message"""
        return self.data["commit"]["message"]

    @property
    def tree(self) -> dict[str, str]:
        """Repository Tree: Files and directories structure"""
        return self.data["commit"]["tree"]

    @property
    def parents(self) -> list[dict[str, str]]:
        """Commit Parents"""
        return self.data["parents"]

    @property
    def stats(self) -> dict[str, int]:
        """Commit Statistics (additions, deletions, total)"""
        return self.data["stats"]

    @property
    def files(self) -> list[dict[str, str]]:
        """Modified Files"""
        return self.data["files"]

    @property
    def repository(self) -> str:
        """Repository Name"""
        return self.url.split("/")[-3]

    # endregion Properties
