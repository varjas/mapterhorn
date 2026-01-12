#!/usr/bin/env python3
"""
Upload processed source datasets to Hugging Face Hub.

This script uploads:
- Source tarball (tar-store/{source}.tar)
- Coverage geopackage (polygon-store/{source}.gpkg)
- Metadata from source-catalog/{source}/metadata.json

Usage:
    python upload_to_hf.py <source_id> [--repo-id REPO_ID] [--token TOKEN]

Examples:
    python upload_to_hf.py au5
    python upload_to_hf.py au5 --repo-id myorg/mapterhorn-sources
    python upload_to_hf.py au5 --token hf_xxx

Environment variables:
    HF_TOKEN: Hugging Face API token (alternative to --token)
    HF_REPO_ID: Default repository ID (alternative to --repo-id)
"""

import argparse
import json
import os
import sys
from pathlib import Path

from validate_source import SourceValidator


def check_huggingface_hub():
    try:
        import huggingface_hub

        return True
    except ImportError:
        return False


def install_huggingface_hub():
    print("Installing huggingface_hub...")
    import subprocess

    subprocess.check_call([sys.executable, "-m", "pip", "install", "huggingface_hub"])
    print("Installation complete!")


def upload_source_to_hf(source_id, repo_id, token=None, skip_validation=False):
    from huggingface_hub import HfApi, CommitOperationAdd

    base_dir = Path(__file__).parent
    source_catalog_dir = base_dir.parent / "source-catalog" / source_id
    tar_store_dir = base_dir / "tar-store"
    polygon_store_dir = base_dir / "polygon-store"

    if not skip_validation:
        print("Running validation checks...\n")
        validator = SourceValidator(source_id, verbose=True)
        if not validator.validate():
            raise ValueError("Validation failed. Fix errors before uploading.")
        print()

        paths = validator.get_file_paths()
        metadata_path = paths["metadata"]
        metadata = validator.get_metadata()
    else:
        metadata_path = source_catalog_dir / "metadata.json"

        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata not found: {metadata_path}")

        with open(metadata_path) as f:
            metadata = json.load(f)

    # Find all tarball files that start with the source_id
    tarball_paths = sorted(tar_store_dir.glob(f"{source_id}_*.tar"))
    # Also include the main tarball if it exists
    main_tarball = tar_store_dir / f"{source_id}.tar"
    if main_tarball.exists():
        tarball_paths.insert(0, main_tarball)

    if not tarball_paths:
        raise FileNotFoundError(
            f"No tarball files found matching pattern: {tar_store_dir}/{source_id}*.tar"
        )

    # Find all geopackage files that start with the source_id
    gpkg_paths = sorted(polygon_store_dir.glob(f"{source_id}_*.gpkg"))
    # Also include the main geopackage if it exists
    main_gpkg = polygon_store_dir / f"{source_id}.gpkg"
    if main_gpkg.exists():
        gpkg_paths.insert(0, main_gpkg)

    if not gpkg_paths:
        raise FileNotFoundError(
            f"No geopackage files found matching pattern: {polygon_store_dir}/{source_id}*.gpkg"
        )

    print(f"Uploading {source_id} to {repo_id}")
    print(f"  Name: {metadata.get('name', 'N/A')}")
    print(f"  Producer: {metadata.get('producer', 'N/A')}")
    print(f"  Resolution: {metadata.get('resolution', 'N/A')}m")
    print(f"  License: {metadata.get('license', 'N/A')}")
    print(f"\nFiles:")
    for tarball_path in tarball_paths:
        print(
            f"  Tarball: {tarball_path} ({tarball_path.stat().st_size / (1024**3):.2f} GB)"
        )
    for gpkg_path in gpkg_paths:
        print(f"  Coverage: {gpkg_path} ({gpkg_path.stat().st_size / 1024:.2f} KB)")

    api = HfApi(token=token)

    operations = []

    # Add all tarball files
    for tarball_path in tarball_paths:
        operations.append(
            CommitOperationAdd(
                path_in_repo=f"{source_id}/{tarball_path.name}",
                path_or_fileobj=str(tarball_path),
            )
        )

    # Add all geopackage files
    for gpkg_path in gpkg_paths:
        operations.append(
            CommitOperationAdd(
                path_in_repo=f"{source_id}/{gpkg_path.name}",
                path_or_fileobj=str(gpkg_path),
            )
        )

    operations.append(
        CommitOperationAdd(
            path_in_repo=f"{source_id}/metadata.json",
            path_or_fileobj=str(metadata_path),
        )
    )

    print("\nUploading to Hugging Face Hub...")

    try:
        api.create_commit(
            repo_id=repo_id,
            operations=operations,
            commit_message=f"Add source dataset: {source_id}",
            repo_type="dataset",
        )
        print(
            f"\n✓ Successfully uploaded {source_id} to https://huggingface.co/datasets/{repo_id}"
        )
        print(
            f"  View at: https://huggingface.co/datasets/{repo_id}/tree/main/{source_id}"
        )
    except Exception as e:
        print(f"\n✗ Upload failed: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Upload processed source datasets to Hugging Face Hub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("source_id", help="Source dataset ID (e.g., au5)")
    parser.add_argument(
        "--repo-id",
        default=os.environ.get("HF_REPO_ID", "mapterhorn/sources"),
        help="Hugging Face repository ID (default: mapterhorn/sources or $HF_REPO_ID)",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("HF_TOKEN"),
        help="Hugging Face API token (default: $HF_TOKEN)",
    )
    parser.add_argument(
        "--install-deps",
        action="store_true",
        help="Install huggingface_hub if not present",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation checks before upload",
    )

    args = parser.parse_args()

    if not check_huggingface_hub():
        if args.install_deps:
            install_huggingface_hub()
        else:
            print("Error: huggingface_hub is not installed.")
            print("Install it with: pip install huggingface_hub")
            print("Or run with --install-deps flag")
            sys.exit(1)

    if not args.token:
        print("Error: No Hugging Face token provided.")
        print("Set HF_TOKEN environment variable or use --token flag")
        print("\nTo get a token:")
        print("1. Go to https://huggingface.co/settings/tokens")
        print("2. Create a new token with 'write' permissions")
        print("3. Export it: export HF_TOKEN=your_token_here")
        sys.exit(1)

    upload_source_to_hf(args.source_id, args.repo_id, args.token, args.skip_validation)


if __name__ == "__main__":
    main()
