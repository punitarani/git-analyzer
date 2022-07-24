""" Test github.commit.py """

import aiohttp
import pandas as pd
import pytest

from git_analyzer.github import Commit


class TestCommit:
    """Test Commit"""

    url = (
        "https://api.github.com/repos/torvalds/linux/commits/"
        "70664fc10c0d722ec79d746d8ac1db8546c94114"
    )

    commit = Commit(url=url)

    def test_init(self):
        """Test Initialization"""

        commit = Commit(url=self.url, auto_get=False)
        assert commit is not None
        assert commit.url == self.url
        assert getattr(commit, "data", None) is None

        # Check auto_get is working
        commit = self.commit
        assert commit.url == self.url
        assert isinstance(commit.data, dict)

    def test_get(self):
        """Test get()"""

        commit = self.commit

        # Delete the data attribute
        commit.data = None

        # Get the commit data
        commit_data = commit.get_commit()

        # Verify the commit data
        assert commit_data is not None
        assert isinstance(commit_data, dict)

    def test_properties(self):
        """Test Properties"""

        commit = self.commit

        # Verify the repository repo
        assert commit.repository == "linux"

        # Verify the commit data
        assert commit.sha == "70664fc10c0d722ec79d746d8ac1db8546c94114"
        assert (
            commit.node_id
            == "C_kwDOACN7MtoAKDcwNjY0ZmMxMGMwZDcyMmVjNzlkNzQ2ZDhhYzFkYjg1NDZjOTQxMTQ"
        )

        # Committer
        assert isinstance(commit.committer, dict)
        assert commit.committer["name"] == "Linus Torvalds"
        assert commit.committer["email"] == "torvalds@linux-foundation.org"
        assert commit.committer["date"] == "2022-07-22T20:02:05Z"

        # Message
        assert isinstance(commit.message, str)
        assert commit.message.startswith("Merge tag 'riscv-for-linus-5.19-rc8'")
        assert commit.message.endswith("kexec: Fix build error without CONFIG_MODULES")

        # Parents
        assert isinstance(commit.parents, list)
        assert len(commit.parents) == 2

        # Stats
        assert isinstance(commit.stats, dict)
        assert commit.stats["additions"] == 10
        assert commit.stats["deletions"] == 9
        assert commit.stats["total"] == 19

        # Files
        assert isinstance(commit.files, list)
        assert len(commit.files) == 8

    def test_parse(self):
        """Test parse()"""

        commit = self.commit

        # Parse the commit
        changes = commit.parse()

        # Verify the dataframe
        assert isinstance(changes, pd.DataFrame)
        assert changes.shape == (8, 10)
        assert changes.columns.tolist() == [
            "sha",
            "filename",
            "status",
            "additions",
            "deletions",
            "changes",
            "blob_url",
            "raw_url",
            "contents_url",
            "patch",
        ]

    def test_get_file_path(self):
        """Test get_file_path()"""

        commit = self.commit

        # Get the commit data
        if commit.data is None:
            commit.get_commit()

        # Get the file path
        file_path = commit.get_file_path()

        # Verify the file path
        assert file_path.parent.parent.parent.exists()
        assert file_path.parent.parent.name == "data"
        assert file_path.parent.name == "linux"
        assert file_path.suffix == ".parquet"

    def test_save(self):
        """Test save()"""

        commit = self.commit

        # Get the commit data if it doesn't exist
        if commit.data is None:
            commit.get_commit()

        # Get the file path where the data will be saved
        file_path = commit.get_file_path()

        # Delete the file if it exists
        file_path.unlink(missing_ok=True)

        # Save the dataframe
        save_path = commit.save()

        # Verify the output path matches the expected
        assert save_path, file_path

        # Verify the file exists
        assert file_path.exists()

    @pytest.mark.asyncio
    async def test_get_async(self):
        """Test get_async()"""

        commit = self.commit

        # Delete the data attribute
        commit.data = None

        # Get the commit data
        async with aiohttp.ClientSession() as session:
            commit_data = await commit.get_commit_async(session)

        # Verify the commit data
        assert commit_data is not None
        assert isinstance(commit_data, dict)

    @pytest.mark.asyncio
    async def test_parse_async(self):
        """Test parse_async()"""

        commit = self.commit

        # Parse the commit
        async with aiohttp.ClientSession() as session:
            changes = await commit.parse_async(session)

        # Verify the dataframe
        assert isinstance(changes, pd.DataFrame)
        assert changes.shape == (8, 10)
        assert changes.columns.tolist() == [
            "sha",
            "filename",
            "status",
            "additions",
            "deletions",
            "changes",
            "blob_url",
            "raw_url",
            "contents_url",
            "patch",
        ]

    @pytest.mark.asyncio
    async def test_save_async(self):
        """Test save_async()"""

        commit = self.commit

        # Create an aiohttp session
        async with aiohttp.ClientSession() as session:
            # Get the commit data if it doesn't exist
            if commit.data is None:
                await commit.get_commit_async(session)

            # Get the file path where the data will be saved
            file_path = commit.get_file_path()

            # Delete the file if it exists
            file_path.unlink(missing_ok=True)

            # Save the dataframe
            save_path = await commit.save_async(session)

            # Verify the output path matches the expected
            assert save_path, file_path

            # Verify the file exists
            assert file_path.exists()
