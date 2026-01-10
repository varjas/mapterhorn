#!/usr/bin/env python3
"""
Create source directories organized by UTM zone from files_by_crs.json

Usage:
    python create_sources_by_crs.py
"""

import json
import shutil
from pathlib import Path

def epsg_to_source_name(epsg_code):
    """Convert EPSG code to source name (e.g., EPSG:26918 -> us18n)"""
    try:
        code = int(epsg_code.split(':')[1])

        # NAD83 UTM North (26900 series)
        if 26900 <= code <= 26999:
            zone = code - 26900
            return f"us{zone:02d}n"

        # WGS84 UTM North (32600 series)
        elif 32600 <= code <= 32699:
            zone = code - 32600
            return f"us{zone:02d}n"

        # Other projections - use EPSG code directly
        else:
            return f"us{code}"
    except:
        return None

def main():
    script_dir = Path(__file__).parent
    catalog_dir = script_dir.parent

    # Read CRS-organized file data
    crs_file = script_dir / "files_by_crs.json"
    if not crs_file.exists():
        print(f"Error: {crs_file} not found!")
        print("Run calculate_size_by_crs.py first.")
        return

    with open(crs_file) as f:
        crs_data = json.load(f)

    print("Creating source directories organized by UTM zone...\n")

    for epsg_code, data in crs_data.items():
        source_name = epsg_to_source_name(epsg_code)
        if not source_name:
            print(f"Warning: Could not determine source name for {epsg_code}, skipping")
            continue

        source_dir = catalog_dir / source_name

        # Create directory
        source_dir.mkdir(exist_ok=True)

        # Write file_list.txt
        file_list_path = source_dir / "file_list.txt"
        with open(file_list_path, 'w') as f:
            f.write('\n'.join(data['files']))

        # Copy metadata.json template if it doesn't exist
        metadata_path = source_dir / "metadata.json"
        if not metadata_path.exists():
            template = {
                "name": f"USGS 3DEP 1m - UTM Zone {epsg_to_source_name(epsg_code)[2:4]}N",
                "website": "https://www.sciencebase.gov/catalog/item/4f70aa9fe4b058caae3f8de5",
                "license": "Public Domain (U.S. Government Work)",
                "producer": "U.S. Geological Survey",
                "resolution": 1.0,
                "access_year": 2025
            }
            with open(metadata_path, 'w') as f:
                json.dump(template, f, indent=2)

        # Create Justfile
        justfile_path = source_dir / "Justfile"
        justfile_content = f"""# Source preparation pipeline
[no-cd]
default:
    uv run python source_download.py {source_name}
    uv run python source_bounds.py {source_name}
    uv run python source_polygonize.py {source_name} 32
    uv run python source_create_tarball.py {source_name}
"""
        with open(justfile_path, 'w') as f:
            f.write(justfile_content)

        # Copy LICENSE if it exists
        license_src = script_dir / "LICENSE.pdf"
        license_dst = source_dir / "LICENSE.pdf"
        if license_src.exists() and not license_dst.exists():
            shutil.copy(license_src, license_dst)

        print(f"✓ Created {source_name}/")
        print(f"    EPSG: {epsg_code}")
        print(f"    Files: {data['file_count']}")
        print(f"    Size: {data['GiB']:.2f} GiB")
        print()

    print("="*60)
    print("Source directories created successfully!")
    print("\nNext steps:")
    print("1. Review the created directories")
    print("2. Edit metadata.json files if needed")
    print("3. Run pipeline for each source:")
    print("   cd mapterhorn/pipelines")
    print("   just ../source-catalog/us18n/")
    print("="*60)

if __name__ == "__main__":
    main()
