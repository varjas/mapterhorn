#!/usr/bin/env python3
"""
Fetch DEM file URLs from USGS The National Map (TNM) Access API.

This script queries the TNM Access API to retrieve download URLs for Digital
Elevation Model (DEM) products. It supports filtering by dataset, geographic
extent, and format, with automatic pagination to retrieve all available files.

Usage:
    uv run python usgs_tnm_fetch_urls.py --dataset "Digital Elevation Model (DEM) 1 meter" --output ../source-catalog/usgs3dep1m/file_list.txt
    uv run python usgs_tnm_fetch_urls.py --dataset "National Elevation Dataset (NED) 1/3 arc-second" --bbox -125,24,-66,50 --output ../source-catalog/usgsned13/file_list.txt
"""

import argparse
import json
import sys
import time
from typing import List, Optional, Dict, Any
import requests


TNM_API_BASE = "https://tnmaccess.nationalmap.gov/api/v1"
PRODUCTS_ENDPOINT = f"{TNM_API_BASE}/products"

# Common USGS elevation datasets
DATASETS = {
    "1m": "Digital Elevation Model (DEM) 1 meter",
    "ned13": "National Elevation Dataset (NED) 1/3 arc-second",
    "ned1": "National Elevation Dataset (NED) 1 arc-second",
    "ned19": "Original Product Resolution (OPR) DEM",
    "ifsar": "Alaska IFSAR 5 meter DEM",
}


def fetch_products(
    dataset: str,
    bbox: Optional[str] = None,
    polygon_type: Optional[str] = None,
    polygon_code: Optional[str] = None,
    prod_format: str = "GeoTIFF",
    offset: int = 0,
    max_results: int = 1000,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Query the TNM Access API for products.

    Args:
        dataset: Dataset name (e.g., "Digital Elevation Model (DEM) 1 meter")
        bbox: Bounding box as "minLon,minLat,maxLon,maxLat" in decimal degrees
        polygon_type: Predefined polygon type (state, huc2, huc4, huc8)
        polygon_code: Code matching the polygon_type
        prod_format: File format (default: GeoTIFF)
        offset: Starting position for pagination
        max_results: Maximum results per page (must be multiple of 5 if offset > 100)
        date_start: Start date filter (YYYY-MM-DD format)
        date_end: End date filter (YYYY-MM-DD format)

    Returns:
        API response as dictionary
    """
    params = {
        "datasets": dataset,
        "prodFormats": prod_format,
        "offset": offset,
        "max": max_results,
        "outputFormat": "JSON",
    }

    if bbox:
        params["bbox"] = bbox
    if polygon_type and polygon_code:
        params["polyType"] = polygon_type
        params["polyCode"] = polygon_code
    if date_start:
        params["start"] = date_start
    if date_end:
        params["end"] = date_end

    print(f"Querying TNM API (offset={offset}, max={max_results})...", file=sys.stderr)

    try:
        response = requests.get(PRODUCTS_ENDPOINT, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error querying API: {e}", file=sys.stderr)
        sys.exit(1)


def extract_download_urls(response: Dict[str, Any]) -> List[str]:
    """
    Extract download URLs from API response.

    Args:
        response: API response dictionary

    Returns:
        List of download URLs
    """
    urls = []
    items = response.get("items", [])

    for item in items:
        # Try multiple URL fields in order of preference
        download_url = None

        # Primary download URL field
        if "downloadURL" in item and item["downloadURL"]:
            download_url = item["downloadURL"]

        # Raster-specific URL
        elif "downloadURLRaster" in item and item["downloadURLRaster"]:
            download_url = item["downloadURLRaster"]

        # Check urls object for format-specific links
        elif "urls" in item and isinstance(item["urls"], dict):
            # Prefer GeoTIFF, then TIFF, then others
            for fmt in ["GeoTIFF", "TIFF", "IMG"]:
                if fmt in item["urls"] and item["urls"][fmt]:
                    download_url = item["urls"][fmt]
                    break

        if download_url:
            urls.append(download_url)
        else:
            # Log items without URLs for debugging
            title = item.get("title", "Unknown")
            print(f"Warning: No download URL found for item: {title}", file=sys.stderr)

    return urls


def deduplicate_tiles(urls: List[str]) -> List[str]:
    """
    Keep only the most recent version of each tile.

    For USGS NED files like USGS_13_n38w110_20241031.tif, this extracts
    the tile identifier (n38w110) and date (20241031), then keeps only
    the URL with the most recent date for each tile.

    Args:
        urls: List of download URLs

    Returns:
        Deduplicated list with only the most recent version of each tile
    """
    import re
    from collections import defaultdict

    # Group URLs by tile identifier
    tile_versions = defaultdict(list)

    for url in urls:
        # Extract filename from URL
        filename = url.split('/')[-1]

        # Try to match USGS tile pattern: USGS_13_n38w110_20241031.tif
        # or similar patterns with tile coordinate and date
        match = re.search(r'(n\d+[ew]\d+)_(\d{8})', filename, re.IGNORECASE)

        if match:
            tile_id = match.group(1).lower()  # e.g., "n38w110"
            date = match.group(2)              # e.g., "20241031"
            tile_versions[tile_id].append((date, url))
        else:
            # If pattern doesn't match, keep the URL as-is
            # (use URL itself as unique identifier)
            tile_versions[url].append(('00000000', url))

    # For each tile, keep only the most recent version
    deduplicated = []
    for tile_id, versions in tile_versions.items():
        # Sort by date (descending) and take the first one
        versions.sort(reverse=True, key=lambda x: x[0])
        most_recent_url = versions[0][1]
        deduplicated.append(most_recent_url)

    return sorted(deduplicated)


def fetch_all_products(
    dataset: str,
    bbox: Optional[str] = None,
    polygon_type: Optional[str] = None,
    polygon_code: Optional[str] = None,
    prod_format: str = "GeoTIFF",
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    max_total: Optional[int] = None,
    deduplicate: bool = True,
) -> List[str]:
    """
    Fetch all product URLs with automatic pagination.

    Args:
        dataset: Dataset name
        bbox: Bounding box filter
        polygon_type: Polygon type filter
        polygon_code: Polygon code filter
        prod_format: File format
        date_start: Start date filter
        date_end: End date filter
        max_total: Maximum total results to retrieve (None for unlimited)
        deduplicate: If True, keep only the most recent version of each tile (default: True)

    Returns:
        List of all download URLs
    """
    all_urls = []
    offset = 0
    max_per_page = 1000

    while True:
        response = fetch_products(
            dataset=dataset,
            bbox=bbox,
            polygon_type=polygon_type,
            polygon_code=polygon_code,
            prod_format=prod_format,
            offset=offset,
            max_results=max_per_page,
            date_start=date_start,
            date_end=date_end,
        )

        # Extract URLs from this page
        urls = extract_download_urls(response)
        all_urls.extend(urls)

        # Print progress
        total_available = response.get("total", 0)
        print(
            f"Retrieved {len(all_urls)} of {total_available} total products",
            file=sys.stderr,
        )

        # Check if we've retrieved all products
        if len(urls) < max_per_page:
            # Last page
            break

        # Check if we've hit the max_total limit
        if max_total and len(all_urls) >= max_total:
            all_urls = all_urls[:max_total]
            break

        # Update offset for next page
        offset += max_per_page

        # Be nice to the API - add a small delay between requests
        time.sleep(0.5)

    # Deduplicate tiles if requested
    if deduplicate:
        original_count = len(all_urls)
        all_urls = deduplicate_tiles(all_urls)
        deduplicated_count = len(all_urls)
        if original_count > deduplicated_count:
            print(
                f"Deduplicated: {original_count} URLs → {deduplicated_count} URLs "
                f"({original_count - deduplicated_count} duplicates removed)",
                file=sys.stderr,
            )

    return all_urls


def main():
    parser = argparse.ArgumentParser(
        description="Fetch DEM file URLs from USGS TNM Access API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch all 1m DEM files for the USA (only most recent version of each tile)
  %(prog)s --dataset 1m --output ../source-catalog/usgs3dep1m/file_list.txt

  # Fetch NED 1/3 arc-second for a specific bounding box
  %(prog)s --dataset ned13 --bbox -125,24,-66,50 --output file_list.txt

  # Fetch data for a specific state (using state code)
  %(prog)s --dataset 1m --poly-type state --poly-code CA --output california.txt

  # Keep all versions of tiles (don't deduplicate)
  %(prog)s --dataset ned13 --bbox -125,24,-66,50 --no-deduplicate --output all_versions.txt

  # List available dataset shortcuts
  %(prog)s --list-datasets
""",
    )

    parser.add_argument(
        "--dataset",
        type=str,
        help="Dataset name or shortcut (use --list-datasets to see options)",
    )

    parser.add_argument(
        "--bbox",
        type=str,
        help="Bounding box: minLon,minLat,maxLon,maxLat (decimal degrees)",
    )

    parser.add_argument(
        "--poly-type",
        type=str,
        choices=["state", "huc2", "huc4", "huc8"],
        help="Polygon type for geographic filtering",
    )

    parser.add_argument(
        "--poly-code",
        type=str,
        help="Polygon code (e.g., state abbreviation like 'CA' for California)",
    )

    parser.add_argument(
        "--format",
        type=str,
        default="GeoTIFF",
        help="Product format (default: GeoTIFF)",
    )

    parser.add_argument(
        "--date-start",
        type=str,
        help="Start date for filtering (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--date-end",
        type=str,
        help="End date for filtering (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--max",
        type=int,
        help="Maximum number of URLs to retrieve",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output file path (default: stdout)",
    )

    parser.add_argument(
        "--list-datasets",
        action="store_true",
        help="List available dataset shortcuts and exit",
    )

    parser.add_argument(
        "--no-deduplicate",
        action="store_true",
        help="Keep all versions of tiles (by default, only the most recent version is kept)",
    )

    args = parser.parse_args()

    # Handle --list-datasets
    if args.list_datasets:
        print("Available dataset shortcuts:")
        for shortcut, full_name in DATASETS.items():
            print(f"  {shortcut:10s} -> {full_name}")
        return

    # Validate required arguments
    if not args.dataset:
        parser.error("--dataset is required (or use --list-datasets)")

    # Resolve dataset name from shortcut if needed
    dataset_name = DATASETS.get(args.dataset, args.dataset)

    print(f"Fetching URLs for dataset: {dataset_name}", file=sys.stderr)
    if args.bbox:
        print(f"Bounding box: {args.bbox}", file=sys.stderr)
    if args.poly_type and args.poly_code:
        print(f"Polygon filter: {args.poly_type}={args.poly_code}", file=sys.stderr)

    # Fetch all URLs
    urls = fetch_all_products(
        dataset=dataset_name,
        bbox=args.bbox,
        polygon_type=args.poly_type,
        polygon_code=args.poly_code,
        prod_format=args.format,
        date_start=args.date_start,
        date_end=args.date_end,
        max_total=args.max,
        deduplicate=not args.no_deduplicate,
    )

    print(f"\nTotal URLs retrieved: {len(urls)}", file=sys.stderr)

    # Write output
    if args.output:
        with open(args.output, "w") as f:
            for url in urls:
                f.write(f"{url}\n")
        print(f"URLs written to: {args.output}", file=sys.stderr)
    else:
        for url in urls:
            print(url)


if __name__ == "__main__":
    main()
