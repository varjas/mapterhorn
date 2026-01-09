import os
import utils
import sys
from pathlib import Path
import requests
from urllib.parse import urlparse


def download_file(url, filepath):
    """
    Download a file using requests library.
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


def download_from_internet(source):
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

    for index, url in enumerate(urls, 1):
        print(f"[{index}/{total_urls}] {url}")
        filename = Path(urlparse(url).path).name
        filepath = source_dir / filename

        download_file(url, filepath)


def main():
    if len(sys.argv) < 2:
        print("ERROR: source argument missing")
        print("Usage: uv run python source_download.py <source_name>")
        sys.exit(1)

    source = sys.argv[1]
    print(f"Downloading {source}...\n")

    utils.create_folder(f"source-store/{source}/")

    try:
        download_from_internet(source)
        print(f"\n✓ SUCCESS: All files for '{source}' downloaded successfully")
    except Exception as e:
        print(f"\n✗ FAILED: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
