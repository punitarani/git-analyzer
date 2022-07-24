""" Test github.github.py """

import aiohttp
import pytest

from git_analyzer.github import GitHub


class TestGitHub:
    """ Test GitHub Class """

    linux_url = 'https://api.github.com/repos/torvalds/linux'

    def test_init(self):
        """ Test Initialization """

        github = GitHub()
        assert github is not None
        assert github.token is not None
        assert github.headers is not None
        assert github.ratelimit_remaining == -1
        assert github.ratelimit_limit == -1

    def test_build_headers(self):
        """ Test build_headers """

        github = GitHub()
        assert github.build_headers() is not None
        assert 'Accept' in github.headers
        assert 'Authorization' in github.headers

    def test_get(self):
        """ Test get """

        github = GitHub()
        assert github.get(self.linux_url) is not None
        assert github.ratelimit_remaining < github.ratelimit_limit
        assert github.ratelimit_remaining > 0
        assert github.ratelimit_limit == 5000

    @pytest.mark.asyncio
    async def test_get_async(self):
        """ Test get_async """

        github = GitHub()

        # Create aiohttp session
        async with aiohttp.ClientSession() as session:
            assert await github.get_async(session, self.linux_url) is not None

        # Verify ratelimit
        assert github.ratelimit_remaining < github.ratelimit_limit
        assert github.ratelimit_remaining > 0
        assert github.ratelimit_limit == 5000
