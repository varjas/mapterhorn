import os
import utils
import sys
from pathlib import Path
from urllib.parse import urlparse
import asyncio
import aiohttp
from aiohttp import ClientTimeout, TCPConnector


class ProgressCounter:
    """Async-safe counter for tracking download progress."""

    def __init__(self, total: int):
        self.completed = 0
        self.total = total
        self.lock = asyncio.Lock()

    async def increment(self) -> int:
        """Increment the counter and return the new value."""
        async with self.lock:
            self.completed += 1
            return self.completed


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    filepath: Path,
    counter: ProgressCounter,
    semaphore: asyncio.Semaphore,
) -> tuple[str, bool, str | None]:
    """
    Download a single file asynchronously.

    Args:
        session: aiohttp ClientSession for making requests.
        url: The URL to download from.
        filepath: The local path where the file will be saved.
        counter: Progress counter for tracking completion.
        semaphore: Semaphore to limit concurrent downloads.

    Returns:
        Tuple of (filename, success, error_message)
    """
    filename = Path(urlparse(url).path).name

    async with semaphore:
        try:
            # Download file with streaming
            async with session.get(url) as response:
                response.raise_for_status()

                # Write file in chunks (1MB for better performance)
                chunk_size = 1024 * 1024
                with open(filepath, "wb") as f:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        f.write(chunk)

            await counter.increment()
            return filename, True, None

        except Exception as e:
            await counter.increment()
            return filename, False, str(e)


async def download_all_files(
    urls: list[str], source_dir: Path, max_concurrent: int = 50
) -> list[tuple[str, str]]:
    """
    Download all files concurrently using asyncio.

    Args:
        urls: List of URLs to download.
        source_dir: Directory to save downloaded files.
        max_concurrent: Maximum number of concurrent downloads.

    Returns:
        List of (url, filename) tuples for failed downloads.
    """
    total_urls = len(urls)
    counter = ProgressCounter(total_urls)
    failed_downloads = []

    # Configure connection limits and timeouts
    timeout = ClientTimeout(total=300, connect=30, sock_read=60)
    connector = TCPConnector(
        limit=max_concurrent,  # Total connection limit
        limit_per_host=30,  # Per-host limit to avoid overwhelming servers
        ttl_dns_cache=300,  # DNS cache TTL
    )

    # Semaphore to limit concurrent downloads
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout
    ) as session:
        # Create all download tasks
        tasks = []
        for url in urls:
            filename = Path(urlparse(url).path).name
            filepath = source_dir / filename
            task = download_file(session, url, filepath, counter, semaphore)
            tasks.append((url, task))

        # Execute all downloads concurrently
        results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

        # Process results and print progress
        for (url, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                filename = Path(urlparse(url).path).name
                print(f"[{counter.completed}/{total_urls}] ✗ {filename} - {str(result)}")
                failed_downloads.append((url, filename))
            else:
                filename, success, error = result
                if success:
                    print(f"[{counter.completed}/{total_urls}] ✓ {filename}")
                else:
                    print(f"[{counter.completed}/{total_urls}] ✗ {filename} - {error}")
                    failed_downloads.append((url, filename))

    return failed_downloads


def download_files(source: str) -> None:
    """
    Download all files listed in the source's file_list.txt.

    Args:
        source: The name of the source (used to locate file_list.txt and determine save location).

    Raises:
        FileNotFoundError: If the file_list.txt does not exist for the source.
        ValueError: If no URLs are found in the file_list.txt.
        RuntimeError: If any downloads fail.
    """
    file_list_path = f"../source-catalog/{source}/file_list.txt"

    if not os.path.exists(file_list_path):
        raise FileNotFoundError(f"File list not found: {file_list_path}")

    urls = []
    with open(file_list_path) as file:
        for line in file.readlines():
            url_string = line.strip()

            # Skip commented lines
            if url_string.startswith("#"):
                continue

            urls.append(url_string)

    if not urls:
        raise ValueError(f"No URLs found in file_list.txt for source '{source}'")

    total_urls = len(urls)
    print(f"Found {total_urls} file(s) to download\n")

    source_dir = Path(f"source-store/{source}")

    # Run async download with high concurrency (50-100 concurrent downloads)
    failed_downloads = asyncio.run(download_all_files(urls, source_dir, max_concurrent=50))

    if failed_downloads:
        error_msg = f"{len(failed_downloads)} file(s) failed to download:\n"
        for url, filename in failed_downloads:
            error_msg += f"  - {filename}: {url}\n"
        raise RuntimeError(error_msg.strip())


def main() -> None:
    """
    Main entry point for the source download script.

    Parses command-line arguments and initiates the download process for a specified source.

    Command-line arguments:
        source_name: The name of the source to download. This should match a directory
            in the source-catalog folder that contains a file_list.txt.

    Usage:
        uv run python source_download.py <source_name>
    """
    if len(sys.argv) < 2:
        print("ERROR: source argument missing")
        print("Usage: uv run python source_download.py <source_name>")
        sys.exit(1)

    source = sys.argv[1]
    print(f"Downloading {source}...\n")

    utils.create_folder(f"source-store/{source}/")

    try:
        download_files(source)
        print(f"\n✓ SUCCESS: All files for '{source}' downloaded successfully")
    except Exception as error:
        print(f"\n✗ FAILED: {str(error)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
