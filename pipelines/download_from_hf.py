#!/usr/bin/env python3
"""
Download files from Hugging Face Hub.

This module provides functionality to download tarballs and their checksums
from Hugging Face Hub datasets. Can be used as a standalone script or imported.

Usage:
    python download_from_hf.py --sources au5a au5b au5c
    python download_from_hf.py --sources-file sources.txt
    python download_from_hf.py --sources au5a --repo-id myorg/mapterhorn
"""

import argparse
import hashlib
import sys
from pathlib import Path

from huggingface_hub import (
    HfApi,
    get_hf_file_metadata,
    hf_hub_download,
    hf_hub_url,
    snapshot_download,
)


DEFAULT_REPO_ID = "varjas/mapterhorn"


def format_bytes(size: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def get_file_size(repo_id: str, filename: str) -> int:
    """Get file size from Hugging Face Hub without downloading."""
    try:
        url = hf_hub_url(repo_id=repo_id, filename=filename, repo_type="dataset")
        metadata = get_hf_file_metadata(url)
        return metadata.size
    except Exception:
        return 0


def get_directory_info(repo_id: str, path: str) -> dict:
    """Get info about files in a directory on Hugging Face Hub."""
    try:
        api = HfApi()
        files = api.list_repo_files(repo_id=repo_id, repo_type="dataset")

        dir_files = [f for f in files if f.startswith(path.rstrip("/") + "/")]

        if not dir_files:
            return {"exists": False, "files": [], "total_size": 0}

        total_size = 0
        for file_path in dir_files:
            size = get_file_size(repo_id, file_path)
            total_size += size

        return {"exists": True, "files": dir_files, "total_size": total_size}
    except Exception:
        return {"exists": False, "files": [], "total_size": 0}


def check_total_size(sources: list[str], repo_id: str) -> dict:
    """Check total download size for all sources."""
    print("Checking file sizes...")

    sizes = {"items": {}, "total": 0, "missing": [], "is_directory": {}}

    for source_id in sources:
        is_dir = "/" in source_id or source_id.endswith("/")
        sizes["is_directory"][source_id] = is_dir

        if is_dir:
            dir_info = get_directory_info(repo_id, source_id)
            if dir_info["exists"]:
                sizes["items"][source_id] = dir_info["total_size"]
                sizes["total"] += dir_info["total_size"]
            else:
                sizes["missing"].append(source_id)
                sizes["items"][source_id] = 0
        else:
            tar_filename = f"{source_id}.tar"
            md5_filename = f"{source_id}.tar.md5"

            tar_size = get_file_size(repo_id, tar_filename)
            md5_size = get_file_size(repo_id, md5_filename)

            if tar_size == 0:
                sizes["missing"].append(source_id)

            sizes["items"][source_id] = tar_size + md5_size
            sizes["total"] += tar_size + md5_size

    return sizes


def compute_md5(file_path: Path) -> str:
    """Compute MD5 checksum of a file."""
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def verify_checksum(tar_file: Path, md5_file: Path) -> bool:
    """Verify that the tar file matches its MD5 checksum."""
    if not md5_file.exists():
        print(f"Warning: Checksum file not found: {md5_file}")
        return False

    expected_md5 = md5_file.read_text().strip().split()[0]
    actual_md5 = compute_md5(tar_file)

    if expected_md5 == actual_md5:
        print(f"✓ Checksum verified: {actual_md5}")
        return True
    else:
        print(f"✗ Checksum mismatch!")
        print(f"  Expected: {expected_md5}")
        print(f"  Actual:   {actual_md5}")
        return False


def download_directory(
    source_id: str,
    repo_id: str,
    output_dir: str = "tar-store",
) -> bool:
    """
    Download an entire directory from Hugging Face Hub.

    Args:
        source_id: Directory path to download
        repo_id: Hugging Face repository ID
        output_dir: Directory to save downloads

    Returns:
        True if download succeeded, False otherwise
    """
    try:
        path = source_id.rstrip("/")
        print(f"Downloading directory {path}/ from {repo_id}...")

        snapshot_download(
            repo_id=repo_id,
            repo_type="dataset",
            allow_patterns=f"{path}/*",
            local_dir=output_dir,
            local_dir_use_symlinks=False,
        )
        print(f"✓ Downloaded directory {path}/")
        return True

    except Exception as e:
        print(f"Error downloading directory from Hugging Face: {e}")
        return False


def download_from_hf(
    source_id: str,
    repo_id: str,
    output_dir: str = "tar-store",
) -> bool:
    """
    Download a tarball and its MD5 checksum from Hugging Face Hub, then verify.

    Args:
        source_id: Identifier for the source (file or directory)
        repo_id: Hugging Face repository ID (e.g., 'username/dataset')
        output_dir: Directory to save downloads (default: 'tar-store')

    Returns:
        True if download and verification succeeded, False otherwise
    """
    is_dir = "/" in source_id or source_id.endswith("/")

    if is_dir:
        return download_directory(source_id, repo_id, output_dir)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    tar_filename = f"{source_id}.tar"
    md5_filename = f"{source_id}.tar.md5"

    try:
        print(f"Downloading {tar_filename} from {repo_id}...")
        downloaded_tar = hf_hub_download(
            repo_id=repo_id,
            filename=tar_filename,
            repo_type="dataset",
            local_dir=output_dir,
            local_dir_use_symlinks=False,
        )
        print(f"✓ Downloaded {tar_filename}")

        print(f"Downloading {md5_filename}...")
        try:
            downloaded_md5 = hf_hub_download(
                repo_id=repo_id,
                filename=md5_filename,
                repo_type="dataset",
                local_dir=output_dir,
                local_dir_use_symlinks=False,
            )
            print(f"✓ Downloaded {md5_filename}")

            print(f"Verifying checksum for {tar_filename}...")
            if not verify_checksum(Path(downloaded_tar), Path(downloaded_md5)):
                print(f"✗ Checksum verification failed for {source_id}")
                return False

        except Exception as e:
            print(f"Warning: Could not download or verify checksum: {e}")
            print("Continuing without verification")

        return True

    except Exception as e:
        print(f"Error downloading from Hugging Face: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Download tarballs from Hugging Face Hub"
    )

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--sources", nargs="+", help="Source IDs to download (e.g., au5a au5b au5c)"
    )
    source_group.add_argument(
        "--sources-file", type=Path, help="File containing source IDs, one per line"
    )

    parser.add_argument(
        "--repo-id",
        default=DEFAULT_REPO_ID,
        help=f"Hugging Face repository ID (default: {DEFAULT_REPO_ID})",
    )
    parser.add_argument(
        "--output-dir",
        default="tar-store",
        help="Directory to save downloads (default: tar-store)",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompt",
    )

    args = parser.parse_args()

    if args.sources:
        sources = args.sources
    elif args.sources_file:
        if not args.sources_file.exists():
            print(f"Error: Sources file not found: {args.sources_file}")
            sys.exit(1)
        sources = [
            line.strip()
            for line in args.sources_file.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    if not sources:
        print("Error: No sources to download")
        sys.exit(1)

    print(f"Sources to download ({len(sources)}):")
    for source in sources:
        print(f"  - {source}")
    print(f"\nRepository: {args.repo_id}")
    print(f"Output directory: {args.output_dir}\n")

    sizes = check_total_size(sources, args.repo_id)

    print(f"\n{'=' * 60}")
    print("Download size summary:")
    print(f"{'=' * 60}")

    for source_id in sources:
        size = sizes["items"].get(source_id, 0)
        is_dir = sizes["is_directory"].get(source_id, False)
        type_label = "dir" if is_dir else "file"

        if size > 0:
            print(f"  {source_id} ({type_label}): {format_bytes(size)}")
        else:
            print(f"  {source_id} ({type_label}): NOT FOUND")

    print(f"\nTotal download size: {format_bytes(sizes['total'])}")

    if sizes["missing"]:
        print(f"\nWarning: {len(sizes['missing'])} source(s) not found on Hub:")
        for source_id in sizes["missing"]:
            print(f"  - {source_id}")

    if not args.yes:
        print(f"\n{'=' * 60}")
        response = input("Proceed with download? [y/N]: ").strip().lower()
        if response not in ["y", "yes"]:
            print("Download cancelled.")
            sys.exit(0)

    print(f"\n{'=' * 60}")
    print("Starting downloads...")
    print(f"{'=' * 60}\n")

    results = {"completed": [], "failed": []}

    for source_id in sources:
        try:
            if download_from_hf(
                source_id=source_id,
                repo_id=args.repo_id,
                output_dir=args.output_dir,
            ):
                results["completed"].append(source_id)
            else:
                results["failed"].append(source_id)
        except KeyboardInterrupt:
            print("\n\nInterrupted by user.")
            sys.exit(1)
        except Exception as e:
            print(f"Error downloading {source_id}: {e}")
            results["failed"].append(source_id)

    print(f"\n{'=' * 60}")
    print("Download complete!")
    print(f"Completed: {len(results['completed'])}")
    print(f"Failed: {len(results['failed'])}")

    if results["completed"]:
        print("\nCompleted sources:")
        for source in results["completed"]:
            print(f"  ✓ {source}")

    if results["failed"]:
        print("\nFailed sources:")
        for source in results["failed"]:
            print(f"  ✗ {source}")

    sys.exit(0 if not results["failed"] else 1)


if __name__ == "__main__":
    main()
