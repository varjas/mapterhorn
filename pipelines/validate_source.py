#!/usr/bin/env python3
"""
Validate processed source datasets.

Checks that all required files exist and meet basic requirements:
- Source tarball exists in tar-store/
- Coverage geopackage exists in polygon-store/
- Metadata.json exists and is valid JSON
- Optional: README.md in source-catalog/

Usage:
    python validate_source.py <source_id> [--verbose]
    python validate_source.py au5
    python validate_source.py au5 --verbose

Exit codes:
    0: All validation checks passed
    1: Validation failed
"""

import argparse
import json
import sys
import tarfile
from pathlib import Path
from urllib.parse import urlparse


class ValidationError(Exception):
    pass


class SourceValidator:
    def __init__(self, source_id, base_dir=None, verbose=False):
        self.source_id = source_id
        self.verbose = verbose

        if base_dir is None:
            base_dir = Path(__file__).parent
        else:
            base_dir = Path(base_dir)

        self.base_dir = base_dir
        self.source_catalog_dir = base_dir.parent / "source-catalog" / source_id
        self.tar_store_dir = base_dir / "tar-store"
        self.polygon_store_dir = base_dir / "polygon-store"

        self.tarball_path = self.tar_store_dir / f"{source_id}.tar"
        self.gpkg_path = self.polygon_store_dir / f"{source_id}.gpkg"
        self.metadata_path = self.source_catalog_dir / "metadata.json"
        self.readme_path = self.source_catalog_dir / "README.md"
        self.file_list_path = self.source_catalog_dir / "file_list.txt"

        self.errors = []
        self.warnings = []
        self._tarball_files = None
        self._bounds_csv = None

    def log(self, message):
        if self.verbose:
            print(f"  {message}")

    def check_tarball(self):
        self.log("Checking tarball...")
        if not self.tarball_path.exists():
            raise ValidationError(f"Tarball not found: {self.tarball_path}")

        size_gb = self.tarball_path.stat().st_size / (1024**3)
        self.log(f"✓ Tarball found: {self.tarball_path.name} ({size_gb:.2f} GB)")

        if size_gb < 0.001:
            self.warnings.append(f"Tarball is very small ({size_gb:.4f} GB)")

        return True

    def check_coverage(self):
        self.log("Checking coverage geopackage...")
        if not self.gpkg_path.exists():
            raise ValidationError(f"Coverage geopackage not found: {self.gpkg_path}")

        size_kb = self.gpkg_path.stat().st_size / 1024
        self.log(f"✓ Coverage geopackage found: {self.gpkg_path.name} ({size_kb:.2f} KB)")

        if size_kb < 1:
            self.warnings.append(f"Coverage geopackage is very small ({size_kb:.2f} KB)")

        return True

    def check_metadata(self):
        self.log("Checking metadata...")
        if not self.metadata_path.exists():
            raise ValidationError(f"Metadata not found: {self.metadata_path}")

        try:
            with open(self.metadata_path) as f:
                metadata = json.load(f)
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON in metadata file: {e}")

        required_fields = ['name', 'producer', 'resolution', 'license']
        missing_fields = [field for field in required_fields if field not in metadata]

        if missing_fields:
            self.errors.append(f"Missing required metadata fields: {', '.join(missing_fields)}")

        optional_fields = ['website', 'access_year']
        missing_optional = [field for field in optional_fields if field not in metadata]
        if missing_optional:
            self.warnings.append(f"Missing optional metadata fields: {', '.join(missing_optional)}")

        self.log(f"✓ Metadata valid: {metadata.get('name', 'N/A')}")
        if self.verbose and 'resolution' in metadata:
            self.log(f"  Resolution: {metadata['resolution']}m")
        if self.verbose and 'license' in metadata:
            self.log(f"  License: {metadata['license']}")

        return metadata

    def check_readme(self):
        self.log("Checking README...")
        if not self.readme_path.exists():
            self.warnings.append(f"README not found: {self.readme_path}")
            return False

        size_bytes = self.readme_path.stat().st_size
        if size_bytes < 100:
            self.warnings.append(f"README is very short ({size_bytes} bytes)")

        self.log(f"✓ README found: {self.readme_path.name}")
        return True

    def _get_tarball_files(self):
        """Extract list of files from tarball (cached)."""
        if self._tarball_files is not None:
            return self._tarball_files

        if not self.tarball_path.exists():
            return None

        try:
            with tarfile.open(self.tarball_path, 'r') as tar:
                self._tarball_files = set(tar.getnames())
            return self._tarball_files
        except Exception as e:
            self.warnings.append(f"Could not read tarball contents: {e}")
            return None

    def _get_bounds_csv(self):
        """Extract and parse bounds.csv from tarball (cached)."""
        if self._bounds_csv is not None:
            return self._bounds_csv

        tarball_files = self._get_tarball_files()
        if tarball_files is None or 'bounds.csv' not in tarball_files:
            return None

        try:
            with tarfile.open(self.tarball_path, 'r') as tar:
                member = tar.getmember('bounds.csv')
                f = tar.extractfile(member)
                lines = f.read().decode('utf-8').strip().split('\n')
                header = lines[0]
                data_lines = lines[1:]

                bounds_data = []
                for line in data_lines:
                    if line.strip():
                        parts = line.split(',')
                        if len(parts) >= 1:
                            bounds_data.append({
                                'filename': parts[0],
                                'line': line
                            })

                self._bounds_csv = bounds_data
                return self._bounds_csv
        except Exception as e:
            self.warnings.append(f"Could not parse bounds.csv from tarball: {e}")
            return None

    def _get_expected_source_prefixes(self):
        """Extract expected file prefixes from file_list.txt URLs."""
        if not self.file_list_path.exists():
            return None

        try:
            with open(self.file_list_path) as f:
                urls = [line.strip() for line in f if line.strip()]

            prefixes = []
            for url in urls:
                parsed = urlparse(url)
                path = parsed.path
                filename = path.split('/')[-1]

                # Remove extension (.zip, .tar, .gz, etc.)
                base = filename.split('.')[0]
                prefixes.append(base)

            return prefixes
        except Exception as e:
            self.warnings.append(f"Could not parse file_list.txt: {e}")
            return None

    def check_tarball_structure(self):
        """Verify tarball has correct structure."""
        self.log("Checking tarball structure...")

        tarball_files = self._get_tarball_files()
        if tarball_files is None:
            self.warnings.append("Could not read tarball structure")
            return False

        required_files = ['metadata.json', 'bounds.csv', 'coverage.gpkg']
        optional_files = ['LICENSE.pdf']

        missing_required = [f for f in required_files if f not in tarball_files]
        if missing_required:
            self.errors.append(f"Tarball missing required files: {', '.join(missing_required)}")
            return False

        missing_optional = [f for f in optional_files if f not in tarball_files]
        if missing_optional:
            self.warnings.append(f"Tarball missing optional files: {', '.join(missing_optional)}")

        tif_files = [f for f in tarball_files if f.startswith('files/') and f.endswith('.tif')]
        if not tif_files:
            self.errors.append("Tarball contains no .tif files in files/ directory")
            return False

        self.log(f"✓ Tarball structure valid: {len(tif_files)} TIF files")
        return True

    def check_source_file_coverage(self):
        """Check that all expected source files were processed."""
        self.log("Checking source file coverage...")

        bounds_data = self._get_bounds_csv()
        if bounds_data is None:
            self.warnings.append("Could not verify source file coverage (bounds.csv unavailable)")
            return True

        expected_prefixes = self._get_expected_source_prefixes()
        if expected_prefixes is None:
            self.warnings.append("Could not verify source file coverage (file_list.txt unavailable)")
            return True

        processed_filenames = {entry['filename'] for entry in bounds_data}

        # Check that we have files from each expected source
        found_prefixes = set()
        for filename in processed_filenames:
            for prefix in expected_prefixes:
                # Check if filename starts with this prefix (case-insensitive)
                if filename.lower().startswith(prefix.lower()):
                    found_prefixes.add(prefix)
                    break

        missing_prefixes = set(expected_prefixes) - found_prefixes
        if missing_prefixes:
            self.warnings.append(
                f"Some source files may not have been processed. "
                f"Missing prefixes: {', '.join(sorted(missing_prefixes))} "
                f"(found {len(found_prefixes)}/{len(expected_prefixes)} sources)"
            )
        else:
            self.log(f"✓ All {len(expected_prefixes)} source file(s) appear to be processed")

        self.log(f"  Total tiles in tarball: {len(processed_filenames)}")

        return True

    def check_bounds_tarball_consistency(self):
        """Verify bounds.csv matches files in tarball."""
        self.log("Checking bounds.csv consistency...")

        tarball_files = self._get_tarball_files()
        bounds_data = self._get_bounds_csv()

        if tarball_files is None or bounds_data is None:
            self.warnings.append("Could not verify bounds.csv consistency")
            return True

        tarball_tifs = {f.replace('files/', '') for f in tarball_files
                       if f.startswith('files/') and f.endswith('.tif')}
        bounds_tifs = {entry['filename'] for entry in bounds_data}

        in_bounds_not_tarball = bounds_tifs - tarball_tifs
        in_tarball_not_bounds = tarball_tifs - bounds_tifs

        if in_bounds_not_tarball:
            self.errors.append(
                f"bounds.csv lists {len(in_bounds_not_tarball)} file(s) not in tarball"
            )
            if self.verbose and len(in_bounds_not_tarball) <= 5:
                for f in sorted(list(in_bounds_not_tarball)[:5]):
                    self.log(f"    Missing: {f}")

        if in_tarball_not_bounds:
            self.errors.append(
                f"Tarball contains {len(in_tarball_not_bounds)} file(s) not in bounds.csv"
            )
            if self.verbose and len(in_tarball_not_bounds) <= 5:
                for f in sorted(list(in_tarball_not_bounds)[:5]):
                    self.log(f"    Unlisted: {f}")

        if not in_bounds_not_tarball and not in_tarball_not_bounds:
            self.log(f"✓ bounds.csv consistent with tarball ({len(bounds_tifs)} files)")
            return True

        return False

    def validate(self, deep_check=True):
        """Run all validation checks. Returns True if valid, False otherwise.

        Args:
            deep_check: If True, perform deep validation including tarball contents
        """
        print(f"Validating source dataset: {self.source_id}")

        try:
            self.check_tarball()
            self.check_coverage()
            metadata = self.check_metadata()
            self.check_readme()

            if deep_check:
                self.check_tarball_structure()
                self.check_bounds_tarball_consistency()
                self.check_source_file_coverage()

            if self.errors:
                print("\n❌ Validation failed with errors:")
                for error in self.errors:
                    print(f"  - {error}")
                return False

            if self.warnings:
                print("\n⚠️  Warnings:")
                for warning in self.warnings:
                    print(f"  - {warning}")

            print(f"\n✅ Validation passed for {self.source_id}")
            return True

        except ValidationError as e:
            print(f"\n❌ Validation failed: {e}")
            return False

    def get_file_paths(self):
        """Return dict of all file paths."""
        return {
            'tarball': self.tarball_path,
            'coverage': self.gpkg_path,
            'metadata': self.metadata_path,
            'readme': self.readme_path,
        }

    def get_metadata(self):
        """Load and return metadata if valid."""
        if not self.metadata_path.exists():
            return None
        try:
            with open(self.metadata_path) as f:
                return json.load(f)
        except:
            return None


def main():
    parser = argparse.ArgumentParser(
        description="Validate processed source datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("source_id", help="Source dataset ID (e.g., au5)")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick validation (skip deep tarball checks)"
    )

    args = parser.parse_args()

    validator = SourceValidator(args.source_id, verbose=args.verbose)

    if validator.validate(deep_check=not args.quick):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
