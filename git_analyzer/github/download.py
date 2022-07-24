""" Bulk Download Commit Data """

import asyncio
import math
from pathlib import Path

import aiohttp
import pandas as pd

from .commit import Commit
from .repository import Repository, RepositoryNotFoundError


class Download:
    """Download Commit Data"""

    data_dir = Path(__file__).parent.parent.parent.joinpath("data")
    encoding_csv = "utf-8-sig"

    def __init__(
        self,
        owner: str,
        name: str,
        number_of_commits: int = 1000,
    ):
        """
        Constructor

        Parameters
        ----------
        owner : str
            GitHub repository owner
        name : str
            GitHub repository repo
        """

        self.owner = owner
        self.repo = name
        self.num_commits = number_of_commits
        self.num_pages = math.ceil(number_of_commits / 100)

        # Initialize the repository
        self.repository = Repository(owner=owner, name=name)

        # Make sure the repository is valid
        if not self.repository.exists:
            raise RepositoryNotFoundError(self.repository.url)

        # Download directory
        self.directory = self.data_dir.joinpath(self.repo)
        # Make sure the directory exists
        self.directory.mkdir(parents=True, exist_ok=True)

        # Cache commits_df: list of the latest commits
        self.commits_df: pd.DataFrame | None = None

    def get_commits_df(self) -> pd.DataFrame:
        """
        Get the latest commits from the repository

        Returns
        -------
        pd.DataFrame
            Dataframe of the latest commits
        """

        # Get the commits
        commits = asyncio.run(
            self.repository.get_commits_async(max_pages=self.num_pages)
        )

        # Cache the commits_df
        self.commits_df = commits.iloc[: self.num_commits]

        # Return the commits
        return commits.iloc[: self.num_commits]

    async def get_commits_df_async(
        self, session: aiohttp.ClientSession | None = None
    ) -> pd.DataFrame:
        """
        Get the latest commits from the repository

        Parameters
        ----------
        session : aiohttp.ClientSession | None
            Aiohttp session object. If None, a new session will be created.

        Returns
        -------
        pd.DataFrame
            Dataframe of the latest commits
        """

        # Get the commits
        commits = await self.repository.get_commits_async(
            session=session, max_pages=self.num_pages
        )

        # Cache the commits_df
        self.commits_df = commits.iloc[: self.num_commits]

        # Return the commits
        return commits.iloc[: self.num_commits]

    def download_commits(self, overwrite: bool = False) -> Path:
        """
        Download the commits to the data folder.

        Parameters
        ----------
        overwrite : bool
            Overwrite the existing files. Default is False

        Returns
        -------
        Path
            Path to the index file of all (old and new) saved commits in the data folder
        """

        # Check if the commits_df is already cached
        if self.commits_df is None:
            self.commits_df = self.get_commits_df()

        # Get the repository url
        url = self.repository.url

        # Store saved file paths
        saved_files = []

        # Iterate through commits_df
        for index, commit in self.commits_df.iterrows():
            # Build the commit url
            commit_url = url + "/commits/" + commit["sha"]

            try:
                # Create a commit object and download the commit
                commit = Commit(url=commit_url, sha=commit["sha"], auto_get=False)

                # Save the commit
                filepath = commit.save(overwrite=overwrite)

                # Add the filepath to the list
                saved_files.append(filepath)
            # Empty commit
            except KeyError:
                print(f"{index}\t - Error downloading commit {commit_url}")

        # Update the index file
        index_path = self.save_index(self.commits_df)

        return index_path

    async def download_commits_async(
        self, session: aiohttp.ClientSession, overwrite: bool = False
    ) -> Path:
        """
        Download the commits asynchronously to the data folder.

        Parameters
        ----------
        session : aiohttp.ClientSession | None
            Aiohttp session object to use.
        overwrite : bool
            Overwrite the existing files. Default is False

        Returns
        -------
        Path
            Path to the index file of all (old and new) saved commits in the data folder
        """

        # Check if the commits_df is already cached
        if self.commits_df is None:
            self.commits_df = await self.get_commits_df_async(session=session)

        # Get the repository url
        url = self.repository.url

        # Store saved file paths
        saved_files = []

        # Iterate through commits_df
        for index, commit in self.commits_df.iterrows():
            # Build the commit url
            commit_url = url + "/commits/" + commit["sha"]

            try:
                # Create a commit object and download the commit
                commit = Commit(url=commit_url, sha=commit["sha"], auto_get=False)

                # Save the commit
                filepath = await commit.save_async(session=session, overwrite=overwrite)
                await asyncio.sleep(0)

                # Add the filepath to the list
                saved_files.append(filepath)
            # Empty commit
            except KeyError:
                print(f"{index}\t - Error downloading commit {commit_url}")

        # Update the index file
        index_path = self.save_index(self.commits_df)

        return index_path

    def save_index(self, commits: pd.DataFrame) -> Path:
        """
        Save the index of the commits to a csv file.
        If index file already exists, it appends the new commits.

        Parameters
        ----------
        commits : pd.DataFrame
            Dataframe of the commits

        Returns
        -------
        Path
            Path to the saved file
        """

        # Get the index file path
        index_path = self.directory.joinpath("index.csv")

        # Check if the index file exists
        if index_path.is_file():
            # Read the index file
            index_df = self.get_index()

            # Remove commits that are already in the index based on sha
            commits = commits.loc[~commits["sha"].isin(index_df["sha"])]

            # Append the new commits to the index file
            commits.to_csv(
                index_path,
                mode="a",
                encoding=self.encoding_csv,
                header=False,
                index=False,
            )

        else:
            # Save the index file
            commits.to_csv(
                index_path,
                mode="w",
                encoding=self.encoding_csv,
                header=True,
                index=False,
            )

        return index_path

    def get_index(self) -> pd.DataFrame:
        """
        Get the index of the commits from the data folder.

        Returns
        -------
        pd.DataFrame
            Dataframe of the commits
        """

        # Get the index file path
        index_path = self.directory.joinpath("index.csv")

        # Check if the index file exists
        if index_path.exists():
            # Read the index file
            index_df = pd.read_csv(
                index_path,
                encoding=self.encoding_csv,
                header=0,
            )

            # Set index column name to 'index'
            index_df.index.name = "index"

            # Return the index
            return index_df

        # Return an empty dataframe
        return pd.DataFrame()
