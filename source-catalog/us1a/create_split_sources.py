#!/usr/bin/env python3
"""
Create sibling source directories organized by lon/lat grid with de-duplication.

Structure: us1ab/, us1ac/, us1ad/, etc. (sequential siblings to us1aa/)
- Longitude-first grouping (keeps same UTM zone together)
- Configurable grouping sizes for lat/lon bands
- De-duplicates files by primary UTM zone for each longitude
- Deterministic directory naming based on sorted grid cells

Usage:
    python create_nested_sources.py [--dry-run] [--lon N] [--lat N]

Examples:
    # Normal run with defaults
    python create_nested_sources.py

    # Dry run to preview metrics without creating directories
    python create_nested_sources.py --dry-run

    # Override grouping sizes
    python create_nested_sources.py --lon 6 --lat 2

    # Dry run with custom grouping
    python create_nested_sources.py --dry-run --lon 3 --lat 1
"""

import json
import argparse
from pathlib import Path
from collections import defaultdict
import numpy as np
import shutil
from grid_utils import deduplicate_files, get_friendly_location, generate_directory_suffix, sort_lon_bands, sort_lat_bands

# Default Configuration
DEFAULT_LON_BAND_GROUPING = (
    3  # Must be a divisor of 6 (1, 2, 3, or 6) to align with UTM zones
)
DEFAULT_LAT_BAND_GROUPING = 2  # Any integer value for latitude grouping

# Validate default configuration
if 6 % DEFAULT_LON_BAND_GROUPING != 0:
    raise ValueError(
        f"DEFAULT_LON_BAND_GROUPING must be a divisor of 6 (1, 2, 3, or 6), got {DEFAULT_LON_BAND_GROUPING}"
    )


def analyze_distribution(all_directories):
    """
    Analyze and display distribution statistics for file count and size across grid cells.
    """
    if not all_directories:
        return

    # Extract metrics
    file_counts = np.array([d["file_count"] for d in all_directories])
    sizes_gib = np.array([d["size_gib"] for d in all_directories])

    print("\n" + "=" * 60)
    print("DISTRIBUTION ANALYSIS")
    print("=" * 60)

    # File count distribution
    print("\n📊 File Count Distribution:")
    print("-" * 60)
    print(f"  Total grid cells: {len(file_counts)}")
    print(f"  Mean:             {np.mean(file_counts):.1f} files")
    print(f"  Median:           {np.median(file_counts):.0f} files")
    print(f"  Std Dev:          {np.std(file_counts):.1f} files")
    print(f"  Min:              {np.min(file_counts)} files")
    print(f"  Max:              {np.max(file_counts)} files")
    print("\n  Percentiles:")
    percentiles = [10, 25, 50, 75, 90, 95, 99]
    for p in percentiles:
        val = np.percentile(file_counts, p)
        print(f"    {p:2d}th: {val:6.0f} files")

    # Create standardized histogram for file counts
    print("\n  Histogram (file count):")
    max_bar_width = 40
    max_file_count_bin = 3000
    file_count_bins = list(range(0, max_file_count_bin + 1, 500))

    # Create histogram with custom bins
    hist_counts = []
    for i in range(len(file_count_bins) - 1):
        lower = file_count_bins[i]
        upper = file_count_bins[i + 1]
        count = np.sum((file_counts >= lower) & (file_counts < upper))
        hist_counts.append(count)

    overflow_count = np.sum(file_counts >= max_file_count_bin)
    hist_counts.append(overflow_count)

    # Find max for scaling
    max_count = max(hist_counts) if hist_counts else 1

    # Print histogram
    for i in range(len(file_count_bins) - 1):
        lower = file_count_bins[i]
        upper = file_count_bins[i + 1]
        count = hist_counts[i]
        bar_width = int((count / max_count) * max_bar_width) if max_count > 0 else 0
        bar = "█" * bar_width
        print(f"    {lower:3d} - {upper:3d}: {bar} ({count})")

    # Print overflow bin
    if overflow_count > 0:
        bar_width = (
            int((overflow_count / max_count) * max_bar_width) if max_count > 0 else 0
        )
        bar = "█" * bar_width
        print(f"    {max_file_count_bin}+     : {bar} ({overflow_count})")

    # Size distribution
    print("\n📊 Size Distribution (GiB):")
    print("-" * 60)
    print(f"  Total grid cells: {len(sizes_gib)}")
    print(f"  Mean:             {np.mean(sizes_gib):.2f} GiB")
    print(f"  Median:           {np.median(sizes_gib):.2f} GiB")
    print(f"  Std Dev:          {np.std(sizes_gib):.2f} GiB")
    print(f"  Min:              {np.min(sizes_gib):.2f} GiB")
    print(f"  Max:              {np.max(sizes_gib):.2f} GiB")
    print("\n  Percentiles:")
    for p in percentiles:
        val = np.percentile(sizes_gib, p)
        print(f"    {p:2d}th: {val:6.2f} GiB")

    # Create standardized histogram for sizes (0-200 GiB in increments of 20)
    print("\n  Histogram (size in GiB):")
    max_size_bin = 1000
    size_bins = list(range(0, max_size_bin + 1, 200))

    # Create histogram with custom bins
    hist_counts_size = []
    for i in range(len(size_bins) - 1):
        lower = size_bins[i]
        upper = size_bins[i + 1]
        count = np.sum((sizes_gib >= lower) & (sizes_gib < upper))
        hist_counts_size.append(count)

    overflow_count_size = np.sum(sizes_gib >= max_size_bin)
    hist_counts_size.append(overflow_count_size)

    # Find max for scaling
    max_count_size = max(hist_counts_size) if hist_counts_size else 1

    # Print histogram
    for i in range(len(size_bins) - 1):
        lower = size_bins[i]
        upper = size_bins[i + 1]
        count = hist_counts_size[i]
        bar_width = (
            int((count / max_count_size) * max_bar_width) if max_count_size > 0 else 0
        )
        bar = "█" * bar_width
        print(f"    {lower:3d} - {upper:3d}: {bar} ({count})")

    # Print overflow bin
    if overflow_count_size > 0:
        bar_width = (
            int((overflow_count_size / max_count_size) * max_bar_width)
            if max_count_size > 0
            else 0
        )
        bar = "█" * bar_width
        print(f"    {max_size_bin}+     : {bar} ({overflow_count_size})")

    # Quartile analysis
    print("\n📈 Quartile Breakdown:")
    print("-" * 60)
    q1_files = np.percentile(file_counts, 25)
    q2_files = np.percentile(file_counts, 50)
    q3_files = np.percentile(file_counts, 75)

    q1_size = np.percentile(sizes_gib, 25)
    q2_size = np.percentile(sizes_gib, 50)
    q3_size = np.percentile(sizes_gib, 75)

    q1_count = np.sum(file_counts <= q1_files)
    q2_count = np.sum((file_counts > q1_files) & (file_counts <= q2_files))
    q3_count = np.sum((file_counts > q2_files) & (file_counts <= q3_files))
    q4_count = np.sum(file_counts > q3_files)

    print("  By File Count:")
    print(f"    Q1 (≤{q1_files:.0f} files):      {q1_count} grid cells")
    print(f"    Q2 ({q1_files:.0f}-{q2_files:.0f} files):  {q2_count} grid cells")
    print(f"    Q3 ({q2_files:.0f}-{q3_files:.0f} files):  {q3_count} grid cells")
    print(f"    Q4 (>{q3_files:.0f} files):      {q4_count} grid cells")

    q1_count_size = np.sum(sizes_gib <= q1_size)
    q2_count_size = np.sum((sizes_gib > q1_size) & (sizes_gib <= q2_size))
    q3_count_size = np.sum((sizes_gib > q2_size) & (sizes_gib <= q3_size))
    q4_count_size = np.sum(sizes_gib > q3_size)

    print("\n  By Size:")
    print(f"    Q1 (≤{q1_size:.2f} GiB):      {q1_count_size} grid cells")
    print(f"    Q2 ({q1_size:.2f}-{q2_size:.2f} GiB):  {q2_count_size} grid cells")
    print(f"    Q3 ({q2_size:.2f}-{q3_size:.2f} GiB):  {q3_count_size} grid cells")
    print(f"    Q4 (>{q3_size:.2f} GiB):      {q4_count_size} grid cells")

    # Outlier analysis (using IQR method)
    print("\n🔍 Outlier Analysis (IQR method):")
    print("-" * 60)

    # File count outliers
    q1_fc = np.percentile(file_counts, 25)
    q3_fc = np.percentile(file_counts, 75)
    iqr_fc = q3_fc - q1_fc
    lower_bound_fc = q1_fc - 1.5 * iqr_fc
    upper_bound_fc = q3_fc + 1.5 * iqr_fc
    outliers_fc = [
        d
        for d in all_directories
        if d["file_count"] < lower_bound_fc or d["file_count"] > upper_bound_fc
    ]

    print("  File Count Outliers:")
    print(f"    IQR: {iqr_fc:.1f}")
    print(f"    Bounds: [{lower_bound_fc:.1f}, {upper_bound_fc:.1f}]")
    print(f"    Outliers: {len(outliers_fc)} grid cells")
    if outliers_fc:
        print("    Top outliers by file count:")
        for d in sorted(outliers_fc, key=lambda x: x["file_count"], reverse=True)[:5]:
            print(
                f"      {d['path']:20s} - {d['file_count']:4d} files, {d['size_gib']:6.2f} GiB"
            )

    # Size outliers
    q1_sz = np.percentile(sizes_gib, 25)
    q3_sz = np.percentile(sizes_gib, 75)
    iqr_sz = q3_sz - q1_sz
    lower_bound_sz = q1_sz - 1.5 * iqr_sz
    upper_bound_sz = q3_sz + 1.5 * iqr_sz
    outliers_sz = [
        d
        for d in all_directories
        if d["size_gib"] < lower_bound_sz or d["size_gib"] > upper_bound_sz
    ]

    print("\n  Size Outliers:")
    print(f"    IQR: {iqr_sz:.2f} GiB")
    print(f"    Bounds: [{lower_bound_sz:.2f}, {upper_bound_sz:.2f}] GiB")
    print(f"    Outliers: {len(outliers_sz)} grid cells")
    if outliers_sz:
        print("    Top outliers by size:")
        for d in sorted(outliers_sz, key=lambda x: x["size_gib"], reverse=True)[:5]:
            print(
                f"      {d['path']:20s} - {d['file_count']:4d} files, {d['size_gib']:6.2f} GiB"
            )

    # Correlation analysis
    print("\n📉 Correlation Analysis:")
    print("-" * 60)
    correlation = np.corrcoef(file_counts, sizes_gib)[0, 1]
    print(f"  File Count vs Size correlation: {correlation:.3f}")
    if correlation > 0.8:
        print("  → Strong positive correlation (size scales linearly with file count)")
    elif correlation > 0.5:
        print("  → Moderate positive correlation")
    else:
        print("  → Weak correlation (file sizes vary significantly)")

    # Average file size per grid cell
    avg_file_sizes = sizes_gib / file_counts * 1024  # Convert to MiB
    print("\n  Average file size per grid cell:")
    print(f"    Mean:   {np.mean(avg_file_sizes):.1f} MiB")
    print(f"    Median: {np.median(avg_file_sizes):.1f} MiB")
    print(f"    Min:    {np.min(avg_file_sizes):.1f} MiB")
    print(f"    Max:    {np.max(avg_file_sizes):.1f} MiB")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Create nested source directories organized by lon/lat grid with de-duplication.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Normal run with defaults
  python create_nested_sources.py
  
  # Dry run to preview metrics without creating directories
  python create_nested_sources.py --dry-run
  
  # Override grouping sizes
  python create_nested_sources.py --lon 6 --lat 2
  
  # Dry run with custom grouping
  python create_nested_sources.py --dry-run --lon 3 --lat 1
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview metrics without creating directories or files",
    )
    parser.add_argument(
        "--lon",
        type=int,
        default=DEFAULT_LON_BAND_GROUPING,
        metavar="N",
        help=f"Longitude grouping in degrees (must be divisor of 6: 1, 2, 3, or 6). Default: {DEFAULT_LON_BAND_GROUPING}",
    )
    parser.add_argument(
        "--lat",
        type=int,
        default=DEFAULT_LAT_BAND_GROUPING,
        metavar="N",
        help=f"Latitude grouping in degrees (any positive integer). Default: {DEFAULT_LAT_BAND_GROUPING}",
    )

    args = parser.parse_args()

    # Validate longitude grouping
    if 6 % args.lon != 0:
        parser.error(f"--lon must be a divisor of 6 (1, 2, 3, or 6), got {args.lon}")

    # Validate latitude grouping
    if args.lat < 1:
        parser.error(f"--lat must be a positive integer, got {args.lat}")

    LON_BAND_GROUPING = args.lon
    LAT_BAND_GROUPING = args.lat
    DRY_RUN = args.dry_run

    script_dir = Path(__file__).parent

    # Read CRS/lon/lat-organized file data
    grid_file = script_dir / "files_by_crs_lon_lat.json"
    if not grid_file.exists():
        print(f"Error: {grid_file} not found!")
        print("Run analyze_file_breakdown.py first.")
        return

    with open(grid_file) as f:
        crs_data = json.load(f)

    if DRY_RUN:
        print("\n" + "=" * 60)
        print("DRY RUN MODE - No directories will be created")
        print("=" * 60)

    print("\nAnalyzing sibling source directories with de-duplication...\n")
    print("Configuration:")
    print(f"  Longitude grouping: {LON_BAND_GROUPING}° bands (UTM-zone aligned)")
    print(f"  Latitude grouping: {LAT_BAND_GROUPING}° bands")
    print(f"  Structure: us1ab/, us1ac/, us1ad/, etc. (siblings to {script_dir.name}/)")
    print("  Longitude bands aligned to UTM boundaries (no mixed CRS)")
    print(
        f"  Mode: {'DRY RUN (preview only)' if DRY_RUN else 'LIVE (creating directories)'}\n"
    )

    # Group files by lon/lat grid (with grouping and de-duplication)
    grid_data = deduplicate_files(crs_data, LON_BAND_GROUPING, LAT_BAND_GROUPING)

    # Create directories
    total_sources = 0
    total_files = 0
    total_bytes = 0
    mixed_crs_directories = []  # Track directories with multiple CRS
    all_directories = []  # Track all directories for stats

    # Get base directory name (e.g., 'us1aa')
    base_dir_name = script_dir.name
    base_prefix = base_dir_name[:-2]  # Remove last two letters (e.g., 'us1aa' -> 'us1')
    parent_dir = script_dir.parent

    # Sort grid cells deterministically (by lon_group then lat_group)
    sorted_grid_keys = sorted(grid_data.keys())

    # Build lon/lat band to index mappings
    # Sort west to east (increasing longitude) and south to north (increasing latitude)
    unique_lon_bands = sort_lon_bands(set(data["lon_group"] for data in grid_data.values()))

    lon_to_index = {lon: idx for idx, lon in enumerate(unique_lon_bands)}

    # Build lat_to_index per longitude band
    lon_lat_to_index = {}
    for lon_band in unique_lon_bands:
        lat_bands_in_lon = sort_lat_bands(
            set(data["lat_group"] for key, data in grid_data.items() if data["lon_group"] == lon_band)
        )
        lon_lat_to_index[lon_band] = {lat: idx for idx, lat in enumerate(lat_bands_in_lon)}

    for grid_key in sorted_grid_keys:
        data = grid_data[grid_key]
        lon_group = data["lon_group"]
        lat_group = data["lat_group"]

        # Generate directory name based on lon/lat band indices
        lon_index = lon_to_index[lon_group]
        lat_index = lon_lat_to_index[lon_group][lat_group]
        dir_suffix = generate_directory_suffix(lon_index, lat_index)
        dir_name = f"{base_prefix}1{dir_suffix}"
        source_dir = parent_dir / dir_name

        if not DRY_RUN:
            source_dir.mkdir(parents=True, exist_ok=True)

        # Write file_list.txt
        file_list_path = source_dir / "file_list.txt"
        unique_files = sorted(set(data["files"]))
        if not DRY_RUN:
            with open(file_list_path, "w") as f:
                f.write("\n".join(unique_files))

        # Get location for display and metadata
        location = get_friendly_location(
            lon_group, lat_group, LON_BAND_GROUPING, LAT_BAND_GROUPING
        )

        # Copy metadata.json from base directory
        metadata_src = script_dir / "metadata.json"
        metadata_dst = source_dir / "metadata.json"
        if metadata_src.exists() and not DRY_RUN:
            shutil.copy2(metadata_src, metadata_dst)

        # Create symlink to Justfile in base directory
        justfile_src = script_dir / "Justfile"
        justfile_dst = source_dir / "Justfile"
        if justfile_src.exists() and not justfile_dst.exists() and not DRY_RUN:
            # Create relative symlink to ../us1aa/Justfile
            justfile_dst.symlink_to(f"../{base_dir_name}/Justfile")

        # Copy LICENSE.pdf from base directory
        license_src = script_dir / "LICENSE.pdf"
        license_dst = source_dir / "LICENSE.pdf"
        if license_src.exists() and not DRY_RUN:
            shutil.copy2(license_src, license_dst)

        # Stats
        file_count = len(unique_files)
        size_gib = data["bytes"] / (1024**3)
        crs_list = sorted(data["crs_codes"])

        # Track directory stats
        dir_info = {
            "path": dir_name,
            "grid_location": f"{lon_group}/{lat_group}",
            "location": location,
            "crs_list": crs_list,
            "file_count": file_count,
            "size_gib": size_gib,
        }
        all_directories.append(dir_info)

        # Check for mixed CRS
        if len(crs_list) > 1:
            mixed_crs_directories.append(dir_info)

        action_verb = "Analyzed" if DRY_RUN else "Created"
        print(f"✓ {action_verb} {dir_name}/")
        print(f"    Grid: {lon_group}/{lat_group}")
        print(f"    Location: {location}")
        print(f"    CRS: {', '.join(crs_list)}")
        print(f"    Files: {file_count}")
        print(f"    Size: {size_gib:.2f} GiB")
        print()

        total_sources += 1
        total_files += file_count
        total_bytes += data["bytes"]

    # Summary
    total_gib = total_bytes / (1024**3)
    total_tib = total_bytes / (1024**4)

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Grid grouping: {LON_BAND_GROUPING}° lon, {LAT_BAND_GROUPING}° lat")
    print(f"Total directories {'analyzed' if DRY_RUN else 'created'}: {total_sources}")
    print(f"Total files: {total_files:,}")
    print(f"Total size: {total_gib:.2f} GiB ({total_tib:.2f} TiB)")
    print(f"Longitude bands: {len(unique_lon_bands)} (letters a-{chr(ord('a') + len(unique_lon_bands) - 1)})")
    print(f"Max latitude bands per longitude: {max(len(lon_lat_to_index[lon]) for lon in unique_lon_bands)}")
    if not DRY_RUN:
        print(f"\nSibling directories created in: {parent_dir}/")
    else:
        print(f"\nSibling directories would be created in: {parent_dir}/")
        print(f"(Run without --dry-run to actually create directories)")

    # Report mixed CRS directories
    if mixed_crs_directories:
        print(
            f"\n⚠️  WARNING: {len(mixed_crs_directories)} directories contain multiple CRS:"
        )
        print("=" * 60)
        for item in mixed_crs_directories:
            print(f"  {item['path']}/")
            print(f"    CRS: {', '.join(item['crs_list'])}")
            print(
                f"    Files: {item['file_count']:,}, Size: {item['size_gib']:.2f} GiB"
            )
        print("=" * 60)
        print("This indicates a potential issue with UTM zone alignment.")
        print("Expected: Each directory should contain only one CRS.")
    else:
        print(f"\n✓ All directories contain single CRS")

    # Run distribution analysis
    analyze_distribution(all_directories)

    # Show largest directories by file count
    print(f"\nLargest directories by file count:")
    print("=" * 60)
    largest_by_files = sorted(
        all_directories, key=lambda x: x["file_count"], reverse=True
    )[:2]
    for i, item in enumerate(largest_by_files, 1):
        print(f"{i}. {item['path']}/")
        print(f"   Location: {item['location']}")
        print(f"   Files: {item['file_count']:,}, Size: {item['size_gib']:.2f} GiB")
        print(f"   CRS: {', '.join(item['crs_list'])}")

    # Show largest directories by size
    print(f"\nLargest directories by size:")
    print("=" * 60)
    largest_by_size = sorted(
        all_directories, key=lambda x: x["size_gib"], reverse=True
    )[:2]
    for i, item in enumerate(largest_by_size, 1):
        print(f"{i}. {item['path']}/")
        print(f"   Location: {item['location']}")
        print(f"   Files: {item['file_count']:,}, Size: {item['size_gib']:.2f} GiB")
        print(f"   CRS: {', '.join(item['crs_list'])}")

    if not DRY_RUN:
        print("\nNext steps:")
        print("1. Review the created directories")
        print("2. Edit metadata.json files if needed")
        print("3. Run pipeline for each source:")
        print("   cd mapterhorn/pipelines")
        print(f"   just ../source-catalog/{base_prefix}ab/")
    else:
        print("\nTo create these directories:")
        print(
            f"  python {Path(__file__).name} --lon {LON_BAND_GROUPING} --lat {LAT_BAND_GROUPING}"
        )
    print("=" * 60)


if __name__ == "__main__":
    main()
