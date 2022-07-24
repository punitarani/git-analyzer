""" Test github.download.py """

from pathlib import Path

import aiohttp
import pytest

from git_analyzer.github import Download


@pytest.mark.downloads
class TestDownload:
    """Test Download Class"""

    num_commits = 100
    download = Download("torvalds", "linux", num_commits)

    def test_init(self):
        """Test Initialization"""

        download = self.download
        assert download is not None
        assert download.owner == "torvalds"
        assert download.repo == "linux"
        assert download.num_commits == self.num_commits
        assert download.num_pages == 1

    def test_get_commits(self):
        """Test get_commits_df"""

        download = self.download
        download.get_commits_df()
        assert download.commits_df is not None
        assert download.commits_df.shape[0] == self.num_commits
        assert download.commits_df.columns.tolist() == [
            "sha",
            "node_id",
            "commit",
            "url",
            "html_url",
            "comments_url",
            "author",
            "committer",
            "parents",
        ]

    @pytest.mark.asyncio
    async def test_get_commits_session(self):
        """Test get_commits_df() with session input"""

        download = self.download

        async with aiohttp.ClientSession() as session:
            await download.get_commits_df_async(session=session)

        assert download.commits_df is not None
        assert download.commits_df.shape[0] == self.num_commits
        assert download.commits_df.columns.tolist() == [
            "sha",
            "node_id",
            "commit",
            "url",
            "html_url",
            "comments_url",
            "author",
            "committer",
            "parents",
        ]

    @pytest.mark.slow
    def test_download_commits(self):
        """Test download_commits()"""

        download = self.download
        download.download_commits(overwrite=False)

        # Get commits_df
        commits_df = download.commits_df

        # Iterate over the file paths and verify they exist
        for sha in commits_df["sha"]:
            file_path = download.directory.joinpath(sha + ".parquet")
            assert Path(file_path).exists()

    @pytest.mark.asyncio
    async def test_download_commits_async(self):
        """Test download_commits_async()"""

        download = self.download
        async with aiohttp.ClientSession() as session:
            await download.download_commits_async(session=session, overwrite=False)

        # Get commits_df
        commits_df = download.commits_df

        # Iterate over the file paths and verify they exist
        for sha in commits_df["sha"]:
            file_path = download.directory.joinpath(sha + ".parquet")
            assert Path(file_path).exists()

    def test_get_index(self):
        """Test get_index()"""

        # Download the latest commit
        download = self.download
        # download.download_commits()

        # Get the index file
        index = download.get_index()
        assert index is not None
        assert index.shape[1] == 9

        # Verify index is up-to-date
        num_files = len(list(Path(download.directory).glob("*.parquet")))
        assert index.shape[0] == num_files
