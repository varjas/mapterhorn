#!/usr/bin/env python3
"""
Create PMTiles tarball and upload to Hugging Face Hub.

This script combines tarball creation and upload in a single workflow.
It creates a tarball of all pmtiles files and uploads it to HF Hub.

Usage:
    python pmtiles_upload.py --repo-id myorg/mapterhorn
    python pmtiles_upload.py --repo-id myorg/mapterhorn --name custom-name
    python pmtiles_upload.py --repo-id myorg/mapterhorn --dry-run
"""

import argparse
import sys

from pmtiles_create_tarball import create_pmtiles_tarball
from upload_to_hf import upload_to_hf


DEFAULT_REPO_ID = "varjas/mapterhorn"


def main():
    parser = argparse.ArgumentParser(
        description="Create PMTiles tarball and upload to Hugging Face Hub"
    )

    parser.add_argument(
        "--name",
        default="pmtiles",
        help="Name for the tarball (default: pmtiles)",
    )
    parser.add_argument(
        "--repo-id",
        default=DEFAULT_REPO_ID,
        help=f"Hugging Face repository ID (default: {DEFAULT_REPO_ID})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually doing it",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation checks before upload",
    )

    args = parser.parse_args()

    print(f"{'=' * 60}")
    print(f"Creating PMTiles Tarball: {args.name}")
    print(f"{'=' * 60}")

    if not create_pmtiles_tarball(name=args.name, dry_run=args.dry_run):
        print(f"\n✗ Failed to create tarball")
        sys.exit(1)

    print(f"\n✓ Tarball creation completed")

    if args.dry_run:
        print(f"\n[DRY RUN] Would upload {args.name} to {args.repo_id}")
        sys.exit(0)

    print(f"\n{'=' * 60}")
    print(f"Uploading to Hugging Face: {args.repo_id}")
    print(f"{'=' * 60}\n")

    if not upload_to_hf(
        source_id=args.name,
        repo_id=args.repo_id,
        tar_store_dir="pmtiles-tar-store",
        skip_validation=args.skip_validation,
    ):
        print(f"\n✗ Failed to upload to Hugging Face")
        sys.exit(1)

    print(f"\n{'=' * 60}")
    print(f"✓ Complete! PMTiles uploaded to {args.repo_id}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
