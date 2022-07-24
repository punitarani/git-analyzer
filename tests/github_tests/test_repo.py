"""
Test github.repository.py
"""

import aiohttp

import pytest

from git_analyzer.github.repository import Repository


class TestRepository:
    """Test Repository Class"""

    repo = Repository("torvalds", "linux")

    def test_init(self):
        """Test Initialization"""

        repo = self.repo

        assert repo is not None
        assert isinstance(repo.token, str)
        assert "Accept" in repo.headers

    def test_exists(self):
        """Test if repository exists"""

        repo = self.repo
        assert repo.url == "https://api.github.com/repos/torvalds/linux"
        assert repo.exists is True

        # Check get() method is working
        assert repo.ratelimit_remaining < repo.ratelimit_limit
        assert repo.ratelimit_remaining > 0

    def test_token_valid(self):
        """Test if token is valid and working"""

        repo = self.repo
        assert "Authorization" in repo.headers
        assert repo.exists is True

        # Verify ratelimit is 5000
        assert repo.ratelimit_limit == 5000

    def test_get_commits(self):
        """Test get_commits"""

        repo = self.repo

        # Get the commits
        commits = repo.get_commits()
        assert commits is not None
        assert commits.shape[0] == 100

    @pytest.mark.slow
    def test_get_commits_multiple_pages(self):
        """Test get_commits with multiple num_pages"""

        repo = self.repo

        # Get the commits
        commits = repo.get_commits(max_pages=10)
        assert commits is not None
        assert commits.shape[0] == 1000

    @pytest.mark.asyncio
    async def test_get_commits_async(self):
        """Test get_commits_async"""

        repo = self.repo

        # Get the commits
        commits = await repo.get_commits_async()
        assert commits is not None
        assert commits.shape[0] == 100

    @pytest.mark.asyncio
    async def test_get_commits_async_multiple_pages(self):
        """Test get_commits_async with multiple num_pages"""

        repo = self.repo

        # Get the commits async
        commits = await repo.get_commits_async(max_pages=10)
        assert commits is not None
        assert commits.shape[0] == 1000

    @pytest.mark.asyncio
    async def test_get_commits_async_session(self):
        """Test get_commits_async with session input"""

        repo = self.repo

        async with aiohttp.ClientSession() as session:
            commits = await repo.get_commits_async(max_pages=10, session=session)
            assert commits is not None
            assert commits.shape[0] == 1000

            # Close the session
            await session.close()
