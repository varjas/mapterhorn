import os
import utils
import sys
from pathlib import Path
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class ProgressCounter:
    """Thread-safe counter for tracking download progress."""

    def __init__(self, total: int):
        self.completed = 0
        self.total = total
        self.lock = threading.Lock()

    def increment(self) -> int:
        """Increment the counter and return the new value."""
        with self.lock:
            self.completed += 1
            return self.completed


def download_file(url: str, filepath: Path) -> None:
    """
    Download a file from a URL and save it to the specified filepath.

    Args:
        url: The URL to download from.
        filepath: The local path where the file will be saved.
    """
    head_response = requests.head(url, allow_redirects=True, timeout=30)
    head_response.raise_for_status()

    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    downloaded_size = 0
    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                downloaded_size += len(chunk)


def download_files(source: str) -> None:
    """
    Download all files listed in the source's file_list.txt.

    Args:
        source: The name of the source (used to locate file_list.txt and determine save location).

    Raises:
        FileNotFoundError: If the file_list.txt does not exist for the source.
        ValueError: If no URLs are found in the file_list.txt.
        requests.exceptions.RequestException: If any HTTP request fails (e.g. connection errors,
            timeouts, HTTP errors).
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
    max_workers = 3
    counter = ProgressCounter(total_urls)

    def download_with_progress(url: str) -> tuple[str, str, bool]:
        """Download a file and return status information."""
        filename = Path(urlparse(url).path).name
        filepath = source_dir / filename
        try:
            download_file(url, filepath)
            completed = counter.increment()
            return filename, f"[{completed}/{total_urls}] ✓ {filename}", True
        except Exception as e:
            completed = counter.increment()
            return filename, f"[{completed}/{total_urls}] ✗ {filename} - {str(e)}", False

    failed_downloads = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_with_progress, url): url for url in urls}

        for future in as_completed(futures):
            filename, message, success = future.result()
            print(message)
            if not success:
                failed_downloads.append((futures[future], filename))

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
