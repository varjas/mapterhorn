import os
import utils
import sys
from pathlib import Path
from urllib.parse import urlparse
import asyncio
import aiohttp
from aiohttp import ClientTimeout, TCPConnector
from tqdm.asyncio import tqdm


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


class PositionManager:
    """Manages progress bar positions for concurrent downloads."""

    def __init__(self, max_positions: int, start_position: int = 2):
        self.available = list(range(start_position, start_position + max_positions))
        self.lock = asyncio.Lock()

    async def acquire(self) -> int:
        """Get an available position."""
        async with self.lock:
            if self.available:
                return self.available.pop(0)
            return 2  # Fallback to position 2

    async def release(self, position: int) -> None:
        """Release a position back to the pool."""
        async with self.lock:
            if position not in self.available:
                self.available.append(position)
                self.available.sort()


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    filepath: Path,
    counter: ProgressCounter,
    semaphore: asyncio.Semaphore,
    position_manager: PositionManager,
    overall_progress: tqdm = None,
) -> tuple[str, bool, str | None]:
    """
    Download a single file asynchronously with progress tracking.

    Args:
        session: aiohttp ClientSession for making requests.
        url: The URL to download from.
        filepath: The local path where the file will be saved.
        counter: Progress counter for tracking completion.
        semaphore: Semaphore to limit concurrent downloads.
        position_manager: Manager for assigning progress bar positions.
        overall_progress: Optional tqdm progress bar for overall progress.

    Returns:
        Tuple of (filename, success, error_message)
    """
    filename = Path(urlparse(url).path).name

    async with semaphore:
        # Acquire a position for this download's progress bar
        position = await position_manager.acquire()
        file_progress = None

        try:
            # Download file with streaming
            async with session.get(url) as response:
                response.raise_for_status()

                # Get file size for individual progress bar
                total_size = int(response.headers.get("content-length", 0))

                # Create individual progress bar for this file
                file_progress = tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=filename[:40],  # Truncate long filenames
                    position=position,
                    leave=False,  # Remove bar when complete
                )

                # Write file in chunks with progress updates
                chunk_size = 1024 * 1024  # 1MB chunks

                with open(filepath, "wb") as f:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        f.write(chunk)

                        # Update both progress bars
                        if file_progress:
                            file_progress.update(len(chunk))
                        if overall_progress:
                            overall_progress.update(len(chunk))

            await counter.increment()
            if file_progress:
                file_progress.close()
            await position_manager.release(position)
            return filename, True, None

        except Exception as e:
            await counter.increment()
            if file_progress:
                file_progress.close()
            await position_manager.release(position)
            return filename, False, str(e)


async def download_all_files(
    urls: list[str], source_dir: Path, max_concurrent: int = 8
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
    # Longer connect timeout for high-concurrency scenarios
    timeout = ClientTimeout(total=300, connect=60, sock_read=90)
    connector = TCPConnector(
        limit=max_concurrent,  # Total connection limit
        limit_per_host=max_concurrent,  # Match total limit for single-host downloads
        ttl_dns_cache=300,  # DNS cache TTL
        force_close=False,  # Keep connections alive for reuse
        enable_cleanup_closed=True,  # Clean up closed connections
    )

    # Semaphore to limit concurrent downloads
    semaphore = asyncio.Semaphore(max_concurrent)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        print("Starting downloads...\n")

        # Don't use overall progress bar - it causes issues with tqdm when total is unknown
        # Individual file progress bars will show the download activity
        overall_progress = None

        # Create file count progress bar (position 0)
        file_count_progress = tqdm(
            total=total_urls,
            unit="file",
            desc="Files Completed",
            position=0,
            leave=True,
        )

        # Create position manager for progress bars (start at position 1 since we removed overall progress)
        position_manager = PositionManager(max_concurrent, start_position=1)

        try:
            # Create all download tasks
            tasks = []
            for url in urls:
                filename = Path(urlparse(url).path).name
                filepath = source_dir / filename
                task = asyncio.create_task(
                    download_file(
                        session,
                        url,
                        filepath,
                        counter,
                        semaphore,
                        position_manager,
                        overall_progress,
                    )
                )
                tasks.append((url, task))

            # Process results as they complete
            for coro in asyncio.as_completed([task for _, task in tasks]):
                try:
                    result = await coro
                    filename, success, error = result
                    file_count_progress.update(1)

                    if not success:
                        tqdm.write(f"✗ {filename} - {error}")
                        # Find the URL for this filename
                        for url, _ in tasks:
                            if Path(urlparse(url).path).name == filename:
                                failed_downloads.append((url, filename))
                                break
                except Exception as e:
                    file_count_progress.update(1)
                    tqdm.write(f"✗ Error - {str(e)}")

        finally:
            file_count_progress.close()

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

    # Run async download with high concurrency
    # Balance between throughput and connection stability
    # Too high causes timeouts, too low wastes bandwidth
    failed_downloads = asyncio.run(
        download_all_files(urls, source_dir, max_concurrent=8)
    )

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
