#!/usr/bin/env python
""" GitHub Repository Commits Download Tool """

import asyncio

import aiohttp
from tabulate import tabulate

from git_analyzer.github import Download, Repository


async def main():
    """Main Function"""

    print("GitHub Repository Commits Download Tool")
    print("======================================")
    print()

    # Get the repository owner and name
    repo_valid = False
    while not repo_valid:
        print("Enter the GitHub repository information:")
        owner: str = input("Repository Owner : ")
        name: str = input("Repository Name  : ")

        # Check if the repository exists
        repository = Repository(owner=owner, name=name)
        if repository.exists:
            repo_valid = True
        else:
            print("\nREPOSITORY DOES NOT EXIST")
            print("--------------------------")
            print()

    # Get the number of commits to download
    # pylint: disable=invalid-name
    num_commits: int = -1
    while num_commits < 0:
        # Get the number of commits
        try:
            num_commits = int(input("Number of commits to download (0-4000): "))

        # Invalid input. i.e. not an integer
        except ValueError:
            print("\nINVALID NUMBER OF COMMITS")
            print("--------------------------")
            print()

        if num_commits < 0:
            print("\nNUMBER OF COMMITS MUST BE POSITIVE")
            print("----------------------------------")
            print()

        if num_commits > 4000:
            num_commits = 4000
            print("Limit is 4000 commits")
            print("---------------------")
            print()

    # Create the downloader
    # noinspection PyUnboundLocalVariable
    downloader = Download(
        owner=owner,
        name=name,
        number_of_commits=num_commits,
    )

    # Print download directory
    print("\nDownload Directory")
    print("------------------")
    print(downloader.directory)

    # Create downloading animation
    downloading_str = f"\nDownloading the latest {num_commits} Commits..."
    print(downloading_str)
    print("-" * (len(downloading_str)-1))

    # Download the commits asynchronously
    async with aiohttp.ClientSession() as session:
        await downloader.download_commits_async(session=session, overwrite=False)

    # Print Finished
    print("Finished\n")

    # Print the folder index
    print("Downloaded Commits Index")
    print("------------------------")
    index_df_sorted = downloader.get_index().sort_values(by="sha", ascending=True)
    print(tabulate(index_df_sorted, headers="keys", tablefmt="psql"))


# Run the downloader
if __name__ == "__main__":
    asyncio.run(main())
