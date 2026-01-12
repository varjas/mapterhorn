#!/usr/bin/env python3
"""
Validate processed source datasets.

Checks that all required files exist and meet basic requirements:
- Source tarball(s) exist in tar-store/ (supports multiple files: {source}*.tar)
- Coverage geopackage(s) exist in polygon-store/ (supports multiple: {source}*.gpkg)
- Metadata.json exists and is valid JSON
- Optional: README.md in source-catalog/

Supports nested source structures (e.g., au5/epsg28350) and can convert them to
flattened format using --convert-to-flat.

Usage:
    python validate_source.py <source_id> [--verbose]
    python validate_source.py au5
    python validate_source.py au5 --verbose
    python validate_source.py au5 --discover-nested
    python validate_source.py au5 --convert-to-flat --verbose

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

        self.metadata_path = self.source_catalog_dir / "metadata.json"
        self.readme_path = self.source_catalog_dir / "README.md"
        self.file_list_path = self.source_catalog_dir / "file_list.txt"

        # Find all matching tarball and gpkg files
        self.tarball_paths = self._find_tarballs()
        self.gpkg_paths = self._find_gpkgs()

        self.errors = []
        self.warnings = []
        self._tarball_files = {}
        self._bounds_csv = {}

    def _find_tarballs(self):
        """Find all tarball files matching the source_id pattern."""
        tarball_paths = sorted(self.tar_store_dir.glob(f"{self.source_id}_*.tar"))
        main_tarball = self.tar_store_dir / f"{self.source_id}.tar"
        if main_tarball.exists():
            tarball_paths.insert(0, main_tarball)
        return tarball_paths

    def _find_gpkgs(self):
        """Find all geopackage files matching the source_id pattern."""
        gpkg_paths = sorted(self.polygon_store_dir.glob(f"{self.source_id}_*.gpkg"))
        main_gpkg = self.polygon_store_dir / f"{self.source_id}.gpkg"
        if main_gpkg.exists():
            gpkg_paths.insert(0, main_gpkg)
        return gpkg_paths

    def log(self, message):
        if self.verbose:
            print(f"  {message}")

    def check_tarball(self):
        self.log("Checking tarball(s)...")
        if not self.tarball_paths:
            raise ValidationError(f"No tarball files found matching pattern: {self.tar_store_dir}/{self.source_id}*.tar")

        total_size_gb = 0
        for tarball_path in self.tarball_paths:
            size_gb = tarball_path.stat().st_size / (1024**3)
            total_size_gb += size_gb
            self.log(f"✓ Tarball found: {tarball_path.name} ({size_gb:.2f} GB)")

            if size_gb < 0.001:
                self.warnings.append(f"Tarball {tarball_path.name} is very small ({size_gb:.4f} GB)")

        if len(self.tarball_paths) > 1:
            self.log(f"  Total: {len(self.tarball_paths)} tarballs, {total_size_gb:.2f} GB")

        return True

    def check_coverage(self):
        self.log("Checking coverage geopackage(s)...")
        if not self.gpkg_paths:
            raise ValidationError(f"No geopackage files found matching pattern: {self.polygon_store_dir}/{self.source_id}*.gpkg")

        total_size_kb = 0
        for gpkg_path in self.gpkg_paths:
            size_kb = gpkg_path.stat().st_size / 1024
            total_size_kb += size_kb
            self.log(f"✓ Coverage geopackage found: {gpkg_path.name} ({size_kb:.2f} KB)")

            if size_kb < 1:
                self.warnings.append(f"Coverage geopackage {gpkg_path.name} is very small ({size_kb:.2f} KB)")

        if len(self.gpkg_paths) > 1:
            self.log(f"  Total: {len(self.gpkg_paths)} geopackages, {total_size_kb:.2f} KB")

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

    def _get_tarball_files(self, tarball_path=None):
        """Extract list of files from tarball (cached).
        
        Args:
            tarball_path: Specific tarball to read. If None, reads the first tarball.
        """
        if tarball_path is None:
            if not self.tarball_paths:
                return None
            tarball_path = self.tarball_paths[0]
        
        tarball_key = str(tarball_path)
        if tarball_key in self._tarball_files:
            return self._tarball_files[tarball_key]

        if not tarball_path.exists():
            return None

        try:
            with tarfile.open(tarball_path, 'r') as tar:
                files = set(tar.getnames())
                self._tarball_files[tarball_key] = files
            return files
        except Exception as e:
            self.warnings.append(f"Could not read tarball contents from {tarball_path.name}: {e}")
            return None

    def _get_bounds_csv(self, tarball_path=None):
        """Extract and parse bounds.csv from tarball (cached).
        
        Args:
            tarball_path: Specific tarball to read. If None, reads the first tarball.
        """
        if tarball_path is None:
            if not self.tarball_paths:
                return None
            tarball_path = self.tarball_paths[0]
        
        tarball_key = str(tarball_path)
        if tarball_key in self._bounds_csv:
            return self._bounds_csv[tarball_key]

        tarball_files = self._get_tarball_files(tarball_path)
        if tarball_files is None or 'bounds.csv' not in tarball_files:
            return None

        try:
            with tarfile.open(tarball_path, 'r') as tar:
                member = tar.getmember('bounds.csv')
                f = tar.extractfile(member)
                lines = f.read().decode('utf-8').strip().split('\n')
                # Skip header line
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

                self._bounds_csv[tarball_key] = bounds_data
                return bounds_data
        except Exception as e:
            self.warnings.append(f"Could not parse bounds.csv from {tarball_path.name}: {e}")
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
        """Verify tarball(s) have correct structure."""
        self.log("Checking tarball structure...")

        all_valid = True
        total_tif_files = 0

        for tarball_path in self.tarball_paths:
            tarball_files = self._get_tarball_files(tarball_path)
            if tarball_files is None:
                self.warnings.append(f"Could not read tarball structure for {tarball_path.name}")
                all_valid = False
                continue

            required_files = ['metadata.json', 'bounds.csv', 'coverage.gpkg']
            optional_files = ['LICENSE.pdf']

            missing_required = [f for f in required_files if f not in tarball_files]
            if missing_required:
                self.errors.append(f"Tarball {tarball_path.name} missing required files: {', '.join(missing_required)}")
                all_valid = False
                continue

            missing_optional = [f for f in optional_files if f not in tarball_files]
            if missing_optional:
                self.warnings.append(f"Tarball {tarball_path.name} missing optional files: {', '.join(missing_optional)}")

            tif_files = [f for f in tarball_files if f.startswith('files/') and f.endswith('.tif')]
            if not tif_files:
                self.errors.append(f"Tarball {tarball_path.name} contains no .tif files in files/ directory")
                all_valid = False
                continue

            total_tif_files += len(tif_files)
            self.log(f"✓ Tarball {tarball_path.name} structure valid: {len(tif_files)} TIF files")

        if len(self.tarball_paths) > 1:
            self.log(f"  Total TIF files across all tarballs: {total_tif_files}")

        return all_valid

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
        """Verify bounds.csv matches files in tarball(s)."""
        self.log("Checking bounds.csv consistency...")

        all_consistent = True
        total_files = 0

        for tarball_path in self.tarball_paths:
            tarball_files = self._get_tarball_files(tarball_path)
            bounds_data = self._get_bounds_csv(tarball_path)

            if tarball_files is None or bounds_data is None:
                self.warnings.append(f"Could not verify bounds.csv consistency for {tarball_path.name}")
                continue

            tarball_tifs = {f.replace('files/', '') for f in tarball_files
                           if f.startswith('files/') and f.endswith('.tif')}
            bounds_tifs = {entry['filename'] for entry in bounds_data}

            in_bounds_not_tarball = bounds_tifs - tarball_tifs
            in_tarball_not_bounds = tarball_tifs - bounds_tifs

            if in_bounds_not_tarball:
                self.errors.append(
                    f"{tarball_path.name}: bounds.csv lists {len(in_bounds_not_tarball)} file(s) not in tarball"
                )
                if self.verbose and len(in_bounds_not_tarball) <= 5:
                    for f in sorted(list(in_bounds_not_tarball)[:5]):
                        self.log(f"    Missing: {f}")
                all_consistent = False

            if in_tarball_not_bounds:
                self.errors.append(
                    f"{tarball_path.name}: Tarball contains {len(in_tarball_not_bounds)} file(s) not in bounds.csv"
                )
                if self.verbose and len(in_tarball_not_bounds) <= 5:
                    for f in sorted(list(in_tarball_not_bounds)[:5]):
                        self.log(f"    Unlisted: {f}")
                all_consistent = False

            if not in_bounds_not_tarball and not in_tarball_not_bounds:
                self.log(f"✓ {tarball_path.name}: bounds.csv consistent ({len(bounds_tifs)} files)")
                total_files += len(bounds_tifs)

        if len(self.tarball_paths) > 1 and all_consistent:
            self.log(f"  Total files across all tarballs: {total_files}")

        return all_consistent

    def validate(self, deep_check=True):
        """Run all validation checks. Returns True if valid, False otherwise.

        Args:
            deep_check: If True, perform deep validation including tarball contents
        """
        print(f"Validating source dataset: {self.source_id}")

        try:
            self.check_tarball()
            self.check_coverage()
            self.check_metadata()
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
            'tarballs': self.tarball_paths,
            'coverages': self.gpkg_paths,
            'metadata': self.metadata_path,
            'readme': self.readme_path,
        }

    def discover_nested_sources(self):
        """Find all nested source directories (up to 2 levels deep) with their own metadata.
        
        Returns:
            List of tuples: (relative_path, metadata_path, justfile_path)
        """
        nested = []
        
        # Check for nested directories up to 2 levels deep
        for depth in [1, 2]:
            pattern = '/'.join(['*'] * depth) + '/metadata.json'
            for metadata_path in self.source_catalog_dir.glob(pattern):
                # Skip the root metadata.json
                if metadata_path.parent == self.source_catalog_dir:
                    continue
                
                rel_path = metadata_path.parent.relative_to(self.source_catalog_dir)
                justfile_path = metadata_path.parent / 'Justfile'
                
                nested.append({
                    'relative_path': str(rel_path),
                    'full_path': metadata_path.parent,
                    'metadata_path': metadata_path,
                    'justfile_path': justfile_path if justfile_path.exists() else None,
                })
        
        return sorted(nested, key=lambda x: x['relative_path'])

    def convert_nested_to_flat(self, dry_run=False):
        """Convert nested source structure to flattened format.
        
        This creates {source_id}_{nested_name}.tar and .gpkg files from nested sources.
        
        Args:
            dry_run: If True, only show what would be done without actually doing it
        
        Returns:
            List of created file pairs: [(tarball_path, gpkg_path), ...]
        """
        nested_sources = self.discover_nested_sources()
        
        if not nested_sources:
            self.log("No nested sources found")
            return []
        
        self.log(f"Found {len(nested_sources)} nested source(s)")
        created_files = []
        
        for nested in nested_sources:
            rel_path = nested['relative_path']
            # Create flattened name: source_id + nested path with / replaced by _
            flat_name = f"{self.source_id}_{rel_path.replace('/', '_')}"
            
            # Look for tarball in aggregation-store or tar-store
            source_tarball = None
            aggregation_dir = self.base_dir.parent / "aggregation-store" / self.source_id / rel_path
            if aggregation_dir.exists():
                tarballs = list(aggregation_dir.glob("*.tar"))
                if tarballs:
                    source_tarball = tarballs[0]
            
            # Look for gpkg in polygon-store nested structure
            source_gpkg = None
            nested_polygon_dir = self.polygon_store_dir / self.source_id / rel_path
            if nested_polygon_dir.exists():
                gpkgs = list(nested_polygon_dir.glob("*.gpkg"))
                if gpkgs:
                    source_gpkg = gpkgs[0]
            
            target_tarball = self.tar_store_dir / f"{flat_name}.tar"
            target_gpkg = self.polygon_store_dir / f"{flat_name}.gpkg"
            
            if dry_run:
                self.log(f"[DRY RUN] Would create: {flat_name}")
                if source_tarball:
                    self.log(f"  Tarball: {source_tarball} -> {target_tarball}")
                if source_gpkg:
                    self.log(f"  GPKG: {source_gpkg} -> {target_gpkg}")
            else:
                import shutil
                
                if source_tarball and source_tarball.exists():
                    self.log(f"Copying {source_tarball.name} -> {target_tarball.name}")
                    shutil.copy2(source_tarball, target_tarball)
                    created_files.append(target_tarball)
                
                if source_gpkg and source_gpkg.exists():
                    self.log(f"Copying {source_gpkg.name} -> {target_gpkg.name}")
                    shutil.copy2(source_gpkg, target_gpkg)
                    created_files.append(target_gpkg)
        
        if not dry_run:
            # Refresh the file lists
            self.tarball_paths = self._find_tarballs()
            self.gpkg_paths = self._find_gpkgs()
            self.log(f"Created {len(created_files)} file(s)")
        
        return created_files

    def get_metadata(self):
        """Load and return metadata if valid."""
        if not self.metadata_path.exists():
            return None
        try:
            with open(self.metadata_path) as f:
                return json.load(f)
        except Exception:
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
    parser.add_argument(
        "--discover-nested",
        action="store_true",
        help="Discover and list nested source directories"
    )
    parser.add_argument(
        "--convert-to-flat",
        action="store_true",
        help="Convert nested source structure to flattened format"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually doing it (for --convert-to-flat)"
    )

    args = parser.parse_args()

    validator = SourceValidator(args.source_id, verbose=args.verbose)

    if args.discover_nested:
        nested = validator.discover_nested_sources()
        if nested:
            print(f"Found {len(nested)} nested source(s):")
            for item in nested:
                print(f"  - {item['relative_path']}")
                if args.verbose:
                    print(f"    Metadata: {item['metadata_path']}")
                    if item['justfile_path']:
                        print(f"    Justfile: {item['justfile_path']}")
        else:
            print("No nested sources found")
        sys.exit(0)
    
    if args.convert_to_flat:
        print(f"Converting nested sources for {args.source_id} to flat structure...")
        created = validator.convert_nested_to_flat(dry_run=args.dry_run)
        if not args.dry_run:
            print(f"\n✅ Created {len(created)} file(s)")
        sys.exit(0)

    if validator.validate(deep_check=not args.quick):
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
