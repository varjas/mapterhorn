#!/usr/bin/env python3
"""
Process and upload data sources to Hugging Face Hub.

This script processes a list of data sources by:
1. Running their Justfile pipeline
2. Uploading the resulting tarball to Hugging Face Hub

Usage:
    python process_sources.py --sources au5a au5b au5c
    python process_sources.py --sources-file sources.txt
    python process_sources.py --sources au5a --repo-id myorg/mapterhorn
"""

import argparse
import subprocess
import sys
from pathlib import Path

from upload_to_hf import upload_to_hf


DEFAULT_SOURCES = [
    "au5a",
    "au5b",
    "au5c",
]

DEFAULT_REPO_ID = "varjas/mapterhorn"


class SourceProcessor:
    def __init__(
        self, repo_id: str, dry_run: bool = False, skip_validation: bool = False, skip_upload: bool = False
    ):
        self.repo_id = repo_id
        self.dry_run = dry_run
        self.skip_validation = skip_validation
        self.skip_upload = skip_upload
        self.pipelines_dir = Path.cwd()
        self.source_catalog_dir = self.pipelines_dir.parent / "source-catalog"

    def run_justfile(self, source_id: str) -> bool:
        justfile = self.source_catalog_dir / source_id / "Justfile"

        if not justfile.exists():
            print(f"✗ Justfile not found: {justfile}")
            return False

        print(f"\n{'=' * 60}")
        print(f"Processing: {source_id}")
        print(f"{'=' * 60}")

        if self.dry_run:
            print(f"[DRY RUN] Would run: just -f {justfile}")
            return True

        cmd = ["just", "-f", str(justfile)]
        result = subprocess.run(cmd, cwd=self.pipelines_dir)
        return result.returncode == 0

    def upload_to_hf_source(self, source_id: str) -> bool:
        print(f"\n{'=' * 60}")
        print(f"Uploading: {source_id}")
        print(f"{'=' * 60}")

        if self.dry_run:
            print(f"[DRY RUN] Would upload {source_id} to {self.repo_id}")
            return True

        return upload_to_hf(
            source_id=source_id,
            repo_id=self.repo_id,
            tar_store_dir="tar-store",
            skip_validation=self.skip_validation,
        )

    def process_source(self, source_id: str) -> bool:
        if not self.run_justfile(source_id):
            print(f"✗ Pipeline failed for {source_id}")
            return False

        print(f"✓ Pipeline completed for {source_id}")

        if self.skip_upload:
            print(f"⊘ Skipping upload for {source_id}")
            return True

        if not self.upload_to_hf_source(source_id):
            print(f"✗ Upload failed for {source_id}")
            return False

        print(f"✓ Upload completed for {source_id}")
        return True

    def process_all(self, sources: list[str]):
        results = {"completed": [], "failed": []}

        for source_id in sources:
            try:
                if self.process_source(source_id):
                    results["completed"].append(source_id)
                else:
                    results["failed"].append(source_id)
            except KeyboardInterrupt:
                print("\n\nInterrupted by user.")
                sys.exit(1)
            except Exception as e:
                print(f"✗ Error processing {source_id}: {e}")
                results["failed"].append(source_id)

        print(f"\n{'=' * 60}")
        print("Processing complete!")
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


def main():
    parser = argparse.ArgumentParser(
        description="Process and upload data sources to Hugging Face Hub"
    )

    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--sources", nargs="+", help="Source IDs to process (e.g., au5a au5b au5c)"
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
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually doing it",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation checks before upload",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip upload step (only run processing pipeline)",
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
    else:
        print(f"No sources specified, using default list: {DEFAULT_SOURCES}")
        sources = DEFAULT_SOURCES

    if not sources:
        print("Error: No sources to process")
        sys.exit(1)

    print(f"Sources to process ({len(sources)}):")
    for source in sources:
        print(f"  - {source}")
    print(f"\nRepository: {args.repo_id}")

    processor = SourceProcessor(
        repo_id=args.repo_id, dry_run=args.dry_run, skip_validation=args.skip_validation, skip_upload=args.skip_upload
    )
    processor.process_all(sources)


if __name__ == "__main__":
    main()
