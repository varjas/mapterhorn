import os
import utils
import sys
import subprocess
import shutil
from pathlib import Path


def check_wget_installed() -> bool:
    """
    Check if wget is installed and available in PATH.

    Returns:
        True if wget is available, False otherwise.
    """
    return shutil.which("wget") is not None


def download_all_files(
    urls: list[str], source_dir: Path, max_concurrent: int = 8
) -> None:
    """
    Download all files using wget with automatic resume support.

    Args:
        urls: List of URLs to download.
        source_dir: Directory to save downloaded files.
        max_concurrent: Maximum number of concurrent downloads (not used with wget).

    Raises:
        RuntimeError: If wget is not installed or download fails.
    """
    if not check_wget_installed():
        raise RuntimeError(
            "wget is not installed. Please install it:\n"
            "  Ubuntu/Debian: sudo apt install wget\n"
            "  Mac: brew install wget\n"
            "  Arch: sudo pacman -S wget"
        )

    # Create input file for URLs
    input_file = source_dir / ".download_urls.txt"
    with open(input_file, "w") as f:
        for url in urls:
            f.write(f"{url}\n")

    print(f"Starting download of {len(urls)} file(s)...\n")

    try:
        # wget options:
        #   -nc: no-clobber (skip if file exists)
        #   -i: input file with URLs
        #   -P: output directory
        #   -t 3: retry 3 times on failure
        #   -T 60: timeout 60 seconds
        #   --progress=bar:force: show progress bar
        result = subprocess.run(
            [
                "wget",
                "-nc",  # Skip if file exists (no-clobber)
                "-i",
                str(input_file),  # Input file with URLs
                "-P",
                str(source_dir),  # Output directory
                "-t",
                "3",  # Retry 3 times
                "-T",
                "60",  # Timeout 60 seconds
                "--progress=bar:force",  # Show progress
            ],
            capture_output=False,
        )

        # Clean up input file
        input_file.unlink(missing_ok=True)

        # Check exit code - wget returns:
        # 0: success (all files downloaded or already exist)
        # 1: generic error
        # 2: parse error
        # 3: file I/O error
        # 4: network failure
        # 5: SSL verification failure
        # 6: authentication failure
        # 7: protocol error
        # 8: server error (404, 500, etc.)
        if result.returncode not in [0, 1]:  # 1 is OK (file exists)
            raise RuntimeError(
                f"wget failed with exit code {result.returncode}. "
                f"This may indicate network issues, server errors, or missing files."
            )

    except subprocess.CalledProcessError as e:
        # Clean up input file
        input_file.unlink(missing_ok=True)
        raise RuntimeError(f"Download failed with exit code {e.returncode}")
    except KeyboardInterrupt:
        print("\n\n⚠ Download interrupted by user")
        # Clean up input file
        input_file.unlink(missing_ok=True)
        sys.exit(1)


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

    # Run download with aria2c (handles concurrency, resume, and retries automatically)
    download_all_files(urls, source_dir, max_concurrent=8)


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
