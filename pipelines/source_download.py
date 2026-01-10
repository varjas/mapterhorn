import os
import utils
import sys
from pathlib import Path
import requests
from urllib.parse import urlparse


def is_xml_error_page(filepath):
    """
    Check if a file is an XML error page.
    """
    try:
        with open(filepath, "rb") as f:
            content = f.read()
            if b"<Error>" in content and b"<Code>" in content:
                return True
            elif content.startswith(b"<?xml") or content.startswith(b"<Error>"):
                return True
    except Exception:
        pass

    return False


def validate_file(filepath, min_size=1000):
    """
    Simple validation: check file exists, is non-empty, and not an XML error page.
    Raises ValueError if file is invalid.
    """
    if not os.path.exists(filepath):
        raise ValueError(f"File does not exist: {filepath}")

    file_size = os.path.getsize(filepath)

    if file_size == 0:
        raise ValueError(f"File is empty (0 bytes): {filepath}")

    if file_size < min_size:
        if is_xml_error_page(filepath):
            raise ValueError(f"XML error page (AccessDenied or NoSuchKey): {filepath}")
        raise ValueError(
            f"File too small ({file_size} bytes), likely not valid data: {filepath}"
        )


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
        filename = Path(urlparse(url).path).name
        filepath = source_dir / filename

        print(f"[{index}/{total_urls}] Downloading: {url}")

        download_file(url, filepath)
        validate_file(filepath)


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
    except Exception as error:
        print(f"\n✗ FAILED: {str(error)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
