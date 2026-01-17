#!/usr/bin/env python3
"""
Create tarball of all PMTiles files.

This script creates a tarball containing all .pmtiles files and their .md5
checksums from the pmtiles-store directory (including subdirectories).

Usage:
    python pmtiles_create_tarball.py
    python pmtiles_create_tarball.py --name custom-name
    python pmtiles_create_tarball.py --dry-run
"""

import argparse
import sys
import tarfile
from pathlib import Path

import utils


def create_pmtiles_tarball(name: str = "pmtiles", dry_run: bool = False) -> bool:
    """
    Create a tarball of all PMTiles files.

    Args:
        name: Name for the tarball (default: 'pmtiles')
        dry_run: Show what would be done without actually doing it

    Returns:
        True if successful, False otherwise
    """
    pmtiles_dir = Path("pmtiles-store")
    tar_store_dir = Path("pmtiles-tar-store")
    tar_file = tar_store_dir / f"{name}.tar"
    md5_file = tar_store_dir / f"{name}.tar.md5"

    if not pmtiles_dir.exists():
        print(f"Error: PMTiles directory not found: {pmtiles_dir}")
        return False

    pmtiles_files = sorted(pmtiles_dir.glob("**/*.pmtiles"))
    md5_files = sorted(pmtiles_dir.glob("**/*.md5"))
    all_files = pmtiles_files + md5_files

    if not all_files:
        print(f"Error: No .pmtiles or .md5 files found in {pmtiles_dir}")
        return False

    print(f"Found {len(pmtiles_files):_} .pmtiles files")
    print(f"Found {len(md5_files):_} .md5 files")
    print(f"Total files to archive: {len(all_files):_}")

    if dry_run:
        print(f"\n[DRY RUN] Would create: {tar_file}")
        print(f"[DRY RUN] Would create: {md5_file}")
        print("\nSample files to include:")
        for filepath in all_files[:10]:
            arcname = filepath.relative_to(pmtiles_dir)
            print(f"  {arcname}")
        if len(all_files) > 10:
            print(f"  ... and {len(all_files) - 10:_} more")
        return True

    utils.create_folder(str(tar_store_dir))

    print(f"\nCreating tarball: {tar_file}")

    try:
        checksum = None
        with open(tar_file, "wb") as f:
            writer = utils.HashWriter(f)
            with tarfile.open(fileobj=writer, mode="w") as tar:
                for j, filepath in enumerate(all_files, 1):
                    if j % 1000 == 0:
                        print(f"{j:_} / {len(all_files):_}")

                    arcname = filepath.relative_to(pmtiles_dir)
                    tar.add(filepath, arcname)

            checksum = writer.md5.hexdigest()

        print(f"✓ Created tarball: {tar_file}")
        print(f"  MD5: {checksum}")

        with open(md5_file, "w") as f:
            f.write(f"{checksum} {name}.tar\n")

        print(f"✓ Created checksum: {md5_file}")
        return True

    except Exception as e:
        print(f"Error creating tarball: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Create tarball of all PMTiles files"
    )

    parser.add_argument(
        "--name",
        default="pmtiles",
        help="Name for the tarball (default: pmtiles)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually doing it",
    )

    args = parser.parse_args()

    success = create_pmtiles_tarball(name=args.name, dry_run=args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
