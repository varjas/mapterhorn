#!/usr/bin/env python3
import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
import rasterio
import sys


@dataclass
class Metrics:
    """Metrics for a collection of raster files."""
    file_count: int = 0
    total_size_bytes: int = 0
    dimensions: set = field(default_factory=set)
    resolutions: set = field(default_factory=set)
    datatypes: set = field(default_factory=set)
    min_x: float = float("inf")
    max_x: float = float("-inf")
    min_y: float = float("inf")
    max_y: float = float("-inf")

    def update(self, src, filepath):
        """Update metrics with data from an open rasterio source."""
        self.file_count += 1
        self.total_size_bytes += filepath.stat().st_size
        self.dimensions.add((src.width, src.height))
        self.resolutions.add((abs(src.transform.a), abs(src.transform.e)))
        self.datatypes.add(src.dtypes[0])

        bounds = src.bounds
        self.min_x = min(self.min_x, bounds.left)
        self.max_x = max(self.max_x, bounds.right)
        self.min_y = min(self.min_y, bounds.bottom)
        self.max_y = max(self.max_y, bounds.top)

def analyze_files(source_dir):
    crs_data = defaultdict(lambda: {"files": [], "metrics": Metrics()})
    failed_files = []

    for tif_file in source_dir.glob("*.tif"):
        try:
            with rasterio.open(tif_file) as src:
                crs_wkt = src.crs.wkt if src.crs else ""
                crs_data[crs_wkt]["files"].append(tif_file.name)
                crs_data[crs_wkt]["metrics"].update(src, tif_file)
        except Exception as e:
            print(f"Error reading {tif_file.name}: {e}", file=sys.stderr)
            failed_files.append(tif_file.name)

    if failed_files:
        raise Exception(f"Failed to open {len(failed_files)} file(s)")

    print(f"Found {len(crs_data)} unique CRS projection(s)")

    return crs_data

def main():
    parser = argparse.ArgumentParser(
        description="Verify CRS projections for dataset files and compute metrics"
    )
    parser.add_argument("dataset", help="Dataset name")
    args = parser.parse_args()

    print(f"Processing: {args.dataset}")

    source_dir = Path(__file__).parent / "source-store" / args.dataset

    if not source_dir.exists():
        print(f"Error: Source directory not found: {source_dir}", file=sys.stderr)
        sys.exit(1)

    analyze_files(source_dir)


if __name__ == "__main__":
    main()
