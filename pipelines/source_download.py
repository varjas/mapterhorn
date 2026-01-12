import os
import utils
import sys
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import httpx
from rich.progress import (
    Progress,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    TextColumn,
)


def download_file(
    client: httpx.Client, url: str, filepath: Path, progress: Progress, task_id
) -> tuple[str, bool, str | None]:
    """
    Download a single file with progress tracking.

    Args:
        client: httpx Client for making requests.
        url: The URL to download from.
        filepath: The local path where the file will be saved.
        progress: Rich Progress instance.
        task_id: Task ID for this download in the progress display.

    Returns:
        Tuple of (filename, success, error_message)
    """
    filename = Path(urlparse(url).path).name

    try:
        with client.stream("GET", url, follow_redirects=True) as response:
            response.raise_for_status()

            # Get file size
            total_size = int(response.headers.get("content-length", 0))
            progress.update(task_id, total=total_size)

            # Download with progress updates
            with open(filepath, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=1024 * 1024):  # 1MB chunks
                    f.write(chunk)
                    progress.update(task_id, advance=len(chunk))

        return filename, True, None

    except Exception as e:
        return filename, False, str(e)


def download_all_files(
    urls: list[str], source_dir: Path, max_concurrent: int = 8
) -> list[tuple[str, str]]:
    """
    Download all files concurrently with beautiful progress display.

    Args:
        urls: List of URLs to download.
        source_dir: Directory to save downloaded files.
        max_concurrent: Maximum number of concurrent downloads.

    Returns:
        List of (url, filename) tuples for failed downloads.
    """
    failed_downloads = []

    # Create httpx client with connection pooling
    client = httpx.Client(
        timeout=httpx.Timeout(60.0, connect=30.0),
        limits=httpx.Limits(
            max_connections=max_concurrent, max_keepalive_connections=max_concurrent
        ),
        follow_redirects=True,
    )

    # Create rich progress display
    with Progress(
        TextColumn("[bold blue]{task.description}", justify="left"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
        expand=True,
    ) as progress:
        # Add overall task
        overall_task = progress.add_task("[cyan]Overall Progress", total=len(urls))

        # Create thread pool for concurrent downloads
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            # Submit all download tasks
            future_to_url = {}
            for url in urls:
                filename = Path(urlparse(url).path).name
                filepath = source_dir / filename

                # Create task for this file
                task_id = progress.add_task(f"[green]{filename[:50]}", total=0)

                # Submit download
                future = executor.submit(
                    download_file, client, url, filepath, progress, task_id
                )
                future_to_url[future] = (url, task_id)

            # Process completed downloads
            for future in as_completed(future_to_url):
                url, task_id = future_to_url[future]
                filename, success, error = future.result()

                # Update overall progress
                progress.update(overall_task, advance=1)

                # Remove individual task
                progress.remove_task(task_id)

                if not success:
                    progress.console.print(f"[red]✗ {filename} - {error}[/red]")
                    failed_downloads.append((url, filename))

    client.close()
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

    # Run download with threading for concurrency
    # Balance between throughput and connection stability
    failed_downloads = download_all_files(urls, source_dir, max_concurrent=8)

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
