import os
import utils
import sys
import subprocess
import argparse
from pathlib import Path


def download_all_files(urls: list[str], source_dir: Path) -> None:
    """
    Download all files using wget with automatic resume support.

    Args:
        urls: List of URLs to download.
        source_dir: Directory to save downloaded files.
    """

    input_file = source_dir / ".download_urls.txt"
    expected_files = []

    with open(input_file, "w") as f:
        for url in urls:
            f.write(f"{url}\n")
            filename = Path(url.split("?")[0]).name
            expected_files.append(source_dir / filename)

    print(f"Starting download of {len(urls)} file(s)...\n")

    try:
        result = subprocess.run(
            [
                "wget",
                "--continue",
                "--input-file",
                str(input_file),
                "--directory-prefix",
                str(source_dir),
                "--tries",
                "3",
                "--timeout",
                "60",
                "--progress=bar:force",
            ],
            capture_output=False,
        )

        input_file.unlink(missing_ok=True)

        missing_files = [f for f in expected_files if not f.exists()]

        if missing_files:
            missing_names = [f.name for f in missing_files]
            raise RuntimeError(
                f"{len(missing_files)} file(s) failed to download:\n"
                + "\n".join(f"  - {name}" for name in missing_names)
            )

        if result.returncode not in [0, 1]:
            raise RuntimeError(
                f"wget failed with exit code {result.returncode}. "
                f"This may indicate network issues, server errors, or missing files."
            )

    except subprocess.CalledProcessError as e:
        input_file.unlink(missing_ok=True)
        raise RuntimeError(f"Download failed with exit code {e.returncode}")
    except KeyboardInterrupt:
        print("\n\n⚠ Download interrupted by user")
        input_file.unlink(missing_ok=True)
        sys.exit(1)


def parse_file_list(file_list_path: str) -> list[str]:
    """
    Parse a file_list.txt and extract URLs.

    Args:
        file_list_path: Path to the file_list.txt file.

    Returns:
        List of URLs found in the file.

    Raises:
        FileNotFoundError: If the file_list.txt does not exist.
        ValueError: If no URLs are found in the file.
    """
    if not os.path.exists(file_list_path):
        raise FileNotFoundError(f"File list not found: {file_list_path}")

    urls = []
    with open(file_list_path) as file:
        for line in file.readlines():
            url_string = line.strip()

            if url_string.startswith("#"):
                continue

            urls.append(url_string)

    if not urls:
        raise ValueError(f"No URLs found in {file_list_path}")

    return urls


def main() -> None:
    """
    Main entry point for the source download script.

    Command-line arguments:
        source: Name of the source to download (required). Must match a directory
            in ../source-catalog/ that contains a file_list.txt.

    Usage:
        uv run python source_download.py <source>

    Examples:
        uv run python source_download.py at1
    """
    parser = argparse.ArgumentParser(
        description="Download files for a source from file_list.txt"
    )
    parser.add_argument("source", help="Source name (must have file_list.txt)")

    args = parser.parse_args()

    print(f"Downloading {args.source}...\n")

    try:
        utils.create_folder(f"source-store/{args.source}/")

        file_list_path = f"../source-catalog/{args.source}/file_list.txt"
        urls = parse_file_list(file_list_path)

        print(f"Found {len(urls)} file(s) to download\n")

        source_dir = Path(f"source-store/{args.source}")
        download_all_files(urls, source_dir)

        print(f"\n✓ SUCCESS: All files for '{args.source}' downloaded successfully")
    except Exception as error:
        print(f"\n✗ FAILED: {str(error)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
