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

from huggingface_hub import hf_hub_download


DEFAULT_REPO_ID = "varjas/mapterhorn"


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


def download_from_hf(
    source_id: str,
    repo_id: str,
    output_dir: str = "tar-store",
) -> bool:
    """
    Download a tarball and its MD5 checksum from Hugging Face Hub, then verify.

    Args:
        source_id: Identifier for the source (used to find tar files)
        repo_id: Hugging Face repository ID (e.g., 'username/dataset')
        output_dir: Directory to save downloads (default: 'tar-store')

    Returns:
        True if download and verification succeeded, False otherwise
    """
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
