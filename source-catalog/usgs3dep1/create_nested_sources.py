#!/usr/bin/env python3
"""
Create nested source directories organized by lon/lat grid with de-duplication.

Structure: usgs3dep1/w074/n40/
- Longitude-first nesting (keeps same UTM zone together)
- Configurable grouping sizes for lat/lon bands
- De-duplicates files by primary UTM zone for each longitude

Usage:
    python create_nested_sources.py [--dry-run] [--lon-grouping N] [--lat-grouping N]

Examples:
    # Normal run with defaults
    python create_nested_sources.py

    # Dry run to preview metrics without creating directories
    python create_nested_sources.py --dry-run

    # Override grouping sizes
    python create_nested_sources.py --lon-grouping 6 --lat-grouping 2

    # Dry run with custom grouping
    python create_nested_sources.py --dry-run --lon-grouping 3 --lat-grouping 1
"""

import json
import math
import argparse
from pathlib import Path
from collections import defaultdict
import numpy as np

# Default Configuration
DEFAULT_LON_BAND_GROUPING = (
    2  # Must be a divisor of 6 (1, 2, 3, or 6) to align with UTM zones
)
DEFAULT_LAT_BAND_GROUPING = 1  # Any integer value for latitude grouping

# Validate default configuration
if 6 % DEFAULT_LON_BAND_GROUPING != 0:
    raise ValueError(
        f"DEFAULT_LON_BAND_GROUPING must be a divisor of 6 (1, 2, 3, or 6), got {DEFAULT_LON_BAND_GROUPING}"
    )


# UTM zone boundaries (for Northern hemisphere)
# UTM zones are 6° wide, centered on meridians
def get_primary_utm_zone(lon_deg):
    """
    Determine primary UTM zone for a given longitude.
    Returns the EPSG code pattern (e.g., 26918 for UTM 18N)

    UTM zones:
    Zone 1: 180°W to 174°W (-180° to -174°)
    Zone 2: 174°W to 168°W (-174° to -168°)
    Zone N: Each zone is 6° wide
    """
    # Calculate UTM zone number (1-60)
    zone = math.floor((lon_deg + 180) / 6) + 1

    # NAD83 UTM North zones: EPSG:269XX where XX is zone
    epsg_code = f"EPSG:269{zone:02d}"
    return epsg_code


def get_utm_aligned_lon_group(lon_deg, grouping_size):
    """
    Get UTM-aligned longitude group name.
    Groups are aligned to UTM zone boundaries (which are 6° wide).

    Examples with grouping_size=6:
    - -156°W is in UTM Zone 5 (boundary at -156°W), returns w156
    - -75°W is in UTM Zone 18 (boundary at -78°W), returns w078

    Examples with grouping_size=2:
    - -156°W -> UTM Zone 5 boundary is -156°W, subdivided as: w156, w158, w160
    - -75°W -> UTM Zone 18 boundary is -78°W, subdivided as: w078, w076, w074
    """
    # Calculate which UTM zone this longitude is in
    zone = math.floor((lon_deg + 180) / 6) + 1

    # Calculate the western boundary of this UTM zone
    west_boundary = -180 + (zone - 1) * 6

    # Calculate offset within the UTM zone (0 to 5 degrees)
    offset_in_zone = lon_deg - west_boundary

    # Round down to nearest multiple of grouping_size within the zone
    group_offset = (offset_in_zone // grouping_size) * grouping_size

    # Calculate the actual group boundary
    group_boundary = west_boundary + group_offset

    # Return the group boundary as the group name
    if group_boundary >= 0:
        return f"e{abs(group_boundary):03d}"
    else:
        return f"w{abs(group_boundary):03d}"


def parse_lon_band(lon_band):
    """Parse longitude band string to degrees (e.g., 'w074' -> -74)"""
    direction = lon_band[0]
    value = int(lon_band[1:])
    return -value if direction == "w" else value


def parse_lat_band(lat_band):
    """Parse latitude band string to degrees (e.g., 'n40' -> 40)"""
    direction = lat_band[0]
    value = int(lat_band[1:])
    return value if direction == "n" else -value


def group_band_name(band_value, is_longitude, grouping_size):
    """
    Generate grouped band name.
    For grouping_size=2: values -74,-75 -> w074; -76,-77 -> w076
    band_value should be signed (negative for W/S, positive for E/N)
    """
    # For negative values (W/S), we need to round DOWN (more negative)
    # For positive values (E/N), we round down normally
    if band_value < 0:
        # Round down (toward more negative) for western/southern hemispheres
        grouped_value = -((abs(band_value) // grouping_size) * grouping_size)
    else:
        grouped_value = (band_value // grouping_size) * grouping_size

    if is_longitude:
        if grouped_value >= 0:
            return f"e{abs(grouped_value):03d}"
        else:
            return f"w{abs(grouped_value):03d}"
    else:
        if grouped_value >= 0:
            return f"n{abs(grouped_value):02d}"
        else:
            return f"s{abs(grouped_value):02d}"


def get_friendly_location(lon_group, lat_group, lon_grouping, lat_grouping):
    """Create human-readable location description"""
    lon_deg = abs(int(lon_group[1:]))
    lat_deg = abs(int(lat_group[1:]))
    lon_dir = "W" if lon_group.startswith("w") else "E"
    lat_dir = "N" if lat_group.startswith("n") else "S"

    # Longitude is UTM-zone aligned
    lon_range = f"{lon_deg}-{lon_deg + lon_grouping}°{lon_dir}"
    lat_range = f"{lat_deg}-{lat_deg + lat_grouping}°{lat_dir}"

    return f"{lat_range}, {lon_range}"


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

    # Create histogram for file counts
    print("\n  Histogram (file count):")
    hist, bin_edges = np.histogram(file_counts, bins=10)
    max_bar_width = 40
    max_count = hist.max()
    for i, (count, edge) in enumerate(zip(hist, bin_edges[:-1])):
        next_edge = bin_edges[i + 1]
        bar_width = int((count / max_count) * max_bar_width) if max_count > 0 else 0
        bar = "█" * bar_width
        print(f"    {edge:6.0f} - {next_edge:6.0f}: {bar} ({count})")

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

    # Create histogram for sizes
    print("\n  Histogram (size in GiB):")
    hist, bin_edges = np.histogram(sizes_gib, bins=10)
    max_count = hist.max()
    for i, (count, edge) in enumerate(zip(hist, bin_edges[:-1])):
        next_edge = bin_edges[i + 1]
        bar_width = int((count / max_count) * max_bar_width) if max_count > 0 else 0
        bar = "█" * bar_width
        print(f"    {edge:6.2f} - {next_edge:6.2f}: {bar} ({count})")

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
  python create_nested_sources.py --lon-grouping 6 --lat-grouping 2
  
  # Dry run with custom grouping
  python create_nested_sources.py --dry-run --lon-grouping 3 --lat-grouping 1
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview metrics without creating directories or files",
    )
    parser.add_argument(
        "--lon-grouping",
        type=int,
        default=DEFAULT_LON_BAND_GROUPING,
        metavar="N",
        help=f"Longitude grouping in degrees (must be divisor of 6: 1, 2, 3, or 6). Default: {DEFAULT_LON_BAND_GROUPING}",
    )
    parser.add_argument(
        "--lat-grouping",
        type=int,
        default=DEFAULT_LAT_BAND_GROUPING,
        metavar="N",
        help=f"Latitude grouping in degrees (any positive integer). Default: {DEFAULT_LAT_BAND_GROUPING}",
    )

    args = parser.parse_args()

    # Validate longitude grouping
    if 6 % args.lon_grouping != 0:
        parser.error(
            f"--lon-grouping must be a divisor of 6 (1, 2, 3, or 6), got {args.lon_grouping}"
        )

    # Validate latitude grouping
    if args.lat_grouping < 1:
        parser.error(
            f"--lat-grouping must be a positive integer, got {args.lat_grouping}"
        )

    LON_BAND_GROUPING = args.lon_grouping
    LAT_BAND_GROUPING = args.lat_grouping
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

    print("\nAnalyzing nested source directories with de-duplication...\n")
    print("Configuration:")
    print(f"  Longitude grouping: {LON_BAND_GROUPING}° bands (UTM-zone aligned)")
    print(f"  Latitude grouping: {LAT_BAND_GROUPING}° bands")
    print("  Structure: usgs3dep1/<lon>/<lat>/")
    print("  Longitude bands aligned to UTM boundaries (no mixed CRS)")
    print(
        f"  Mode: {'DRY RUN (preview only)' if DRY_RUN else 'LIVE (creating directories)'}\n"
    )

    # Group files by lon/lat grid (with grouping and de-duplication)
    grid_data = defaultdict(
        lambda: {
            "files": [],
            "total_bytes": 0,
            "crs_codes": set(),
            "lon_group": None,
            "lat_group": None,
        }
    )

    # First pass: collect all files with their metadata
    file_metadata = []  # (url, lon_deg, lat_deg, epsg_code, size)

    for epsg_code, epsg_data in crs_data.items():
        for lon_band, lon_data in epsg_data.get("lon_bands", {}).items():
            lon_deg = parse_lon_band(lon_band)

            for lat_band, lat_data in lon_data.get("lat_bands", {}).items():
                lat_deg = parse_lat_band(lat_band)

                for file_url in lat_data["files"]:
                    file_metadata.append(
                        {
                            "url": file_url,
                            "lon_deg": lon_deg,
                            "lat_deg": lat_deg,
                            "epsg_code": epsg_code,
                            "size": lat_data["bytes"]
                            / len(lat_data["files"]),  # Approximate per-file size
                        }
                    )

    # Second pass: de-duplicate and group
    # Track which files we've already added to avoid duplicates
    seen_files = set()

    for file_info in file_metadata:
        url = file_info["url"]
        lon_deg = file_info["lon_deg"]
        lat_deg = file_info["lat_deg"]
        epsg_code = file_info["epsg_code"]

        # Skip duplicates
        if url in seen_files:
            continue

        # Check if this is the primary UTM zone for this longitude
        primary_epsg = get_primary_utm_zone(lon_deg)
        if epsg_code != primary_epsg:
            continue

        # This file should be included - mark as seen
        seen_files.add(url)

        # Generate grouped names
        lon_group = get_utm_aligned_lon_group(
            lon_deg, LON_BAND_GROUPING
        )  # UTM-aligned longitude
        lat_group = group_band_name(
            lat_deg, False, LAT_BAND_GROUPING
        )  # Standard lat grouping

        # Create grid key
        grid_key = f"{lon_group}/{lat_group}"

        # Add file to this grid cell
        grid_data[grid_key]["files"].append(url)
        grid_data[grid_key]["total_bytes"] += file_info["size"]
        grid_data[grid_key]["crs_codes"].add(epsg_code)
        grid_data[grid_key]["lon_group"] = lon_group
        grid_data[grid_key]["lat_group"] = lat_group

    # Create directories
    total_sources = 0
    total_files = 0
    total_bytes = 0
    mixed_crs_directories = []  # Track directories with multiple CRS
    all_directories = []  # Track all directories for stats

    for grid_key, data in sorted(grid_data.items()):
        lon_group = data["lon_group"]
        lat_group = data["lat_group"]

        # Create nested directory structure
        lon_dir = script_dir / lon_group
        source_dir = lon_dir / lat_group
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

        # Create symlink to metadata.json
        metadata_src = script_dir / "metadata.json"
        metadata_dst = source_dir / "metadata.json"
        if metadata_src.exists() and not metadata_dst.exists() and not DRY_RUN:
            # Create relative symlink (../../metadata.json from nested dir)
            metadata_dst.symlink_to("../../metadata.json")

        # Create Justfile
        justfile_path = source_dir / "Justfile"
        # Source name for pipeline is the relative path
        source_name = f"usgs3dep1/{lon_group}/{lat_group}"
        justfile_content = f"""# Source preparation pipeline
[no-cd]
default:
    uv run python source_download.py {source_name}
    uv run python source_bounds.py {source_name}
    uv run python source_polygonize.py {source_name} 32
    uv run python source_create_tarball.py {source_name}
"""
        if not DRY_RUN:
            with open(justfile_path, "w") as f:
                f.write(justfile_content)

        # Create symlink to LICENSE if it exists
        license_src = script_dir / "LICENSE.pdf"
        license_dst = source_dir / "LICENSE.pdf"
        if license_src.exists() and not license_dst.exists() and not DRY_RUN:
            # Create relative symlink (../../LICENSE.pdf from nested dir)
            license_dst.symlink_to("../../LICENSE.pdf")

        # Stats
        file_count = len(unique_files)
        size_gib = data["total_bytes"] / (1024**3)
        crs_list = sorted(data["crs_codes"])

        # Track directory stats
        dir_info = {
            "path": f"{lon_group}/{lat_group}",
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
        print(f"✓ {action_verb} {lon_group}/{lat_group}/")
        print(f"    Location: {location}")
        print(f"    CRS: {', '.join(crs_list)}")
        print(f"    Files: {file_count}")
        print(f"    Size: {size_gib:.2f} GiB")
        print()

        total_sources += 1
        total_files += file_count
        total_bytes += data["total_bytes"]

    # Summary
    total_gib = total_bytes / (1024**3)
    total_tib = total_bytes / (1024**4)

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"Grid grouping: {LON_BAND_GROUPING}° lon, {LAT_BAND_GROUPING}° lat")
    print(f"Total grid cells {'analyzed' if DRY_RUN else 'created'}: {total_sources}")
    print(f"Total files: {total_files:,}")
    print(f"Total size: {total_gib:.2f} GiB ({total_tib:.2f} TiB)")
    if not DRY_RUN:
        print(f"\nNested structure created in: {script_dir}/")
    else:
        print(f"\nNested structure would be created in: {script_dir}/")
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
        print("   just ../source-catalog/usgs3dep1/w074/n40/")
    else:
        print("\nTo create these directories:")
        print(
            f"  python {Path(__file__).name} --lon-grouping {LON_BAND_GROUPING} --lat-grouping {LAT_BAND_GROUPING}"
        )
    print("=" * 60)


if __name__ == "__main__":
    main()
