#!/usr/bin/env python3
"""
Upload files to Hugging Face Hub.

This module provides functionality to upload tarballs and their checksums
to Hugging Face Hub datasets. Can be used as a standalone script or imported.

Usage:
    python upload_to_hf.py source_id --repo-id myorg/mapterhorn
    python upload_to_hf.py source_id --repo-id myorg/mapterhorn --skip-validation
"""

import argparse
import sys
from pathlib import Path

from huggingface_hub import HfApi


def upload_to_hf(
    source_id: str,
    repo_id: str,
    tar_store_dir: str = "tar-store",
    skip_validation: bool = False,
) -> bool:
    """
    Upload a tarball and its MD5 checksum to Hugging Face Hub.

    Args:
        source_id: Identifier for the source (used to find tar files)
        repo_id: Hugging Face repository ID (e.g., 'username/dataset')
        tar_store_dir: Directory containing the tarballs (default: 'tar-store')
        skip_validation: Skip validation checks before upload

    Returns:
        True if upload succeeded, False otherwise
    """
    tar_file = Path(tar_store_dir) / f"{source_id}.tar"
    md5_file = Path(tar_store_dir) / f"{source_id}.tar.md5"

    if not tar_file.exists():
        print(f"Error: Tarball not found: {tar_file}")
        return False

    if not md5_file.exists():
        print(f"Warning: MD5 checksum not found: {md5_file}")
        if not skip_validation:
            print("Use --skip-validation to upload without checksum")
            return False

    try:
        api = HfApi()

        print(f"Uploading {tar_file.name} to {repo_id}...")
        api.upload_file(
            path_or_fileobj=str(tar_file),
            path_in_repo=tar_file.name,
            repo_id=repo_id,
            repo_type="dataset",
        )
        print(f"✓ Uploaded {tar_file.name}")

        if md5_file.exists():
            print(f"Uploading {md5_file.name}...")
            api.upload_file(
                path_or_fileobj=str(md5_file),
                path_in_repo=md5_file.name,
                repo_id=repo_id,
                repo_type="dataset",
            )
            print(f"✓ Uploaded {md5_file.name}")

        return True

    except Exception as e:
        print(f"Error uploading to Hugging Face: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Upload tarballs to Hugging Face Hub"
    )

    parser.add_argument(
        "source_id",
        help="Source identifier (e.g., au5a, pmtiles)",
    )
    parser.add_argument(
        "--repo-id",
        required=True,
        help="Hugging Face repository ID (e.g., username/dataset)",
    )
    parser.add_argument(
        "--tar-store-dir",
        default="tar-store",
        help="Directory containing tarballs (default: tar-store)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation checks before upload",
    )

    args = parser.parse_args()

    success = upload_to_hf(
        source_id=args.source_id,
        repo_id=args.repo_id,
        tar_store_dir=args.tar_store_dir,
        skip_validation=args.skip_validation,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
