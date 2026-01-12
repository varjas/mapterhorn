import os
import utils
import sys
import subprocess
from pathlib import Path


def download_all_files(urls: list[str], source_dir: Path, max_threads: int = 5) -> None:
    """
    Download all files using wget with automatic resume support.

    Args:
        urls: List of URLs to download.
        source_dir: Directory to save downloaded files.
        max_threads: Maximum number of concurrent download threads (default: 5).
    """

    input_file = source_dir / ".download_urls.txt"
    expected_files = []

    with open(input_file, "w") as f:
        for url in urls:
            f.write(f"{url}\n")
            filename = Path(url.split('?')[0]).name
            expected_files.append(source_dir / filename)

    print(f"Starting download of {len(urls)} file(s) with {max_threads} parallel threads...\n")

    try:
        result = subprocess.run(
            [
                "wget",
                "-c",
                "-i",
                str(input_file),
                "-P",
                str(source_dir),
                "-t",
                "3",
                "-T",
                "60",
                "--max-threads",
                str(max_threads),
                "--progress=bar:force",
            ],
            capture_output=False,
        )

        input_file.unlink(missing_ok=True)

        missing_files = [f for f in expected_files if not f.exists()]

        if missing_files:
            missing_names = [f.name for f in missing_files]
            raise RuntimeError(
                f"{len(missing_files)} file(s) failed to download:\n" +
                "\n".join(f"  - {name}" for name in missing_names)
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


def download_files(source: str, max_threads: int = 5) -> None:
    """
    Download all files listed in the source's file_list.txt.

    Args:
        source: The name of the source (used to locate file_list.txt and determine save location).
        max_threads: Maximum number of concurrent download threads (default: 5).

    Raises:
        FileNotFoundError: If the file_list.txt does not exist for the source.
        ValueError: If no URLs are found in the file_list.txt.
    """
    file_list_path = f"../source-catalog/{source}/file_list.txt"

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
        raise ValueError(f"No URLs found in file_list.txt for source '{source}'")

    print(f"Found {len(urls)} file(s) to download\n")

    source_dir = Path(f"source-store/{source}")

    download_all_files(urls, source_dir, max_threads)


def main() -> None:
    """
    Main entry point for the source download script.

    Parses command-line arguments and initiates the download process for a specified source.

    Command-line arguments:
        source_name: The name of the source to download. This should match a directory
            in the source-catalog folder that contains a file_list.txt.
        --max-threads: Optional. Maximum number of concurrent download threads (default: 5).

    Usage:
        uv run python source_download.py <source_name> [--max-threads N]
    """
    if len(sys.argv) < 2:
        print("ERROR: source argument missing")
        print("Usage: uv run python source_download.py <source_name> [--max-threads N]")
        sys.exit(1)

    source = sys.argv[1]
    max_threads = 5

    if len(sys.argv) > 2:
        if sys.argv[2] == "--max-threads" and len(sys.argv) > 3:
            try:
                max_threads = int(sys.argv[3])
                if max_threads < 1:
                    print("ERROR: --max-threads must be >= 1")
                    sys.exit(1)
            except ValueError:
                print("ERROR: --max-threads must be a valid integer")
                sys.exit(1)
        else:
            print("ERROR: Invalid arguments")
            print("Usage: uv run python source_download.py <source_name> [--max-threads N]")
            sys.exit(1)

    print(f"Downloading {source}...\n")

    utils.create_folder(f"source-store/{source}/")

    try:
        download_files(source, max_threads)
        print(f"\n✓ SUCCESS: All files for '{source}' downloaded successfully")
    except Exception as error:
        print(f"\n✗ FAILED: {str(error)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
