#!/usr/bin/env python3
"""
Compare multiple pmtiles outputs to analyze differences.

This script samples tiles from pmtiles archives and performs:
- Pixel-by-pixel difference analysis
- Statistical comparisons (mean, std, min, max differences)
- Histogram comparisons
"""

import argparse
import numpy as np
import rasterio
from pathlib import Path
import sys
from collections import defaultdict
from pmtiles.reader import Reader, MmapSource
import random
import time


def sample_tiles_from_pmtiles(pmtiles_path, zoom_level=None, sample_count=50):
    """
    Sample tiles from a pmtiles file by trying a range of tile coordinates.
    """
    with open(pmtiles_path, 'rb') as f:
        reader = Reader(MmapSource(f))
        header = reader.header()

        min_zoom = header['min_zoom']
        max_zoom = header['max_zoom']

        if zoom_level is not None:
            if zoom_level < min_zoom or zoom_level > max_zoom:
                print(f"Warning: zoom {zoom_level} not in range [{min_zoom}, {max_zoom}]")
                return []
            target_zoom = zoom_level
        else:
            target_zoom = max_zoom

        # If bounds are specified in header, use them
        if header['min_lon_e7'] != 0 or header['max_lon_e7'] != 0:
            return get_tiles_from_bounds(reader, header, target_zoom, sample_count)

        # Otherwise, sample by trying tiles in a reasonable range
        print(f"No bounds in header, sampling tiles at zoom {target_zoom}...")
        return sample_tiles_by_scanning(reader, target_zoom, sample_count)


def get_tiles_from_bounds(reader, header, target_zoom, sample_count):
    """Get tiles based on geographic bounds in header."""
    import math

    min_lon = header['min_lon_e7'] / 10000000.0
    min_lat = header['min_lat_e7'] / 10000000.0
    max_lon = header['max_lon_e7'] / 10000000.0
    max_lat = header['max_lat_e7'] / 10000000.0

    def lon_to_tile_x(lon, zoom):
        return int((lon + 180) / 360 * (2 ** zoom))

    def lat_to_tile_y(lat, zoom):
        lat_rad = math.radians(lat)
        return int((1 - math.asinh(math.tan(lat_rad)) / math.pi) / 2 * (2 ** zoom))

    min_x = lon_to_tile_x(min_lon, target_zoom)
    max_x = lon_to_tile_x(max_lon, target_zoom)
    min_y = lat_to_tile_y(max_lat, target_zoom)
    max_y = lat_to_tile_y(min_lat, target_zoom)

    tiles = []
    for x in range(min_x, max_x + 1):
        for y in range(min_y, max_y + 1):
            tile_data = reader.get(target_zoom, x, y)
            if tile_data:
                tiles.append((target_zoom, x, y))

    if sample_count and len(tiles) > sample_count:
        step = max(1, len(tiles) // sample_count)
        tiles = tiles[::step][:sample_count]

    return tiles


def sample_tiles_by_scanning(reader, target_zoom, sample_count):
    """
    Sample tiles by scanning through possible tile coordinates.
    Uses a smart sampling strategy based on zoom level.
    """
    max_tiles = 2 ** target_zoom
    tiles = []

    # For efficiency, we'll sample in a grid pattern
    # Sample more densely in the center (where tiles are more likely)
    sample_density = max(1, int(max_tiles / 100))

    attempts = 0
    max_attempts = sample_count * 100  # Try up to 100x the sample count

    print(f"Scanning with density {sample_density} (checking ~{(max_tiles//sample_density)**2} positions)...")

    for x in range(0, max_tiles, sample_density):
        for y in range(0, max_tiles, sample_density):
            if attempts >= max_attempts:
                break

            attempts += 1

            if attempts % 1000 == 0:
                print(f"  Checked {attempts} positions, found {len(tiles)} tiles so far...")

            tile_data = reader.get(target_zoom, x, y)
            if tile_data:
                tiles.append((target_zoom, x, y))
                print(f"  Found tile {len(tiles)}/{sample_count}: {target_zoom}/{x}/{y}")

                if len(tiles) >= sample_count:
                    print(f"Reached target of {sample_count} tiles after {attempts} attempts")
                    return tiles

        if attempts >= max_attempts:
            break

    # If we didn't find enough tiles with sparse sampling, try random sampling
    if len(tiles) < sample_count and len(tiles) < 100:
        print(f"\nSparse sampling found {len(tiles)} tiles, trying random sampling...")
        for i in range(max_attempts):
            if i % 1000 == 0 and i > 0:
                print(f"  Random sampling: {i} attempts, {len(tiles)} tiles found...")

            x = random.randint(0, max_tiles - 1)
            y = random.randint(0, max_tiles - 1)
            tile_data = reader.get(target_zoom, x, y)
            if tile_data and (target_zoom, x, y) not in tiles:
                tiles.append((target_zoom, x, y))
                print(f"  Found tile {len(tiles)}/{sample_count}: {target_zoom}/{x}/{y}")
                if len(tiles) >= sample_count:
                    break

    print(f"Completed sampling: found {len(tiles)} tiles\n")
    return tiles


def extract_tile(pmtiles_path, z, x, y):
    """Extract a single tile from pmtiles as bytes."""
    with open(pmtiles_path, 'rb') as f:
        reader = Reader(MmapSource(f))
        tile_data = reader.get(z, x, y)
        return tile_data


def load_geotiff_as_array(tile_bytes):
    """Load a GeoTIFF tile from bytes as a numpy array."""
    if tile_bytes is None:
        return None

    from rasterio.io import MemoryFile

    try:
        with MemoryFile(tile_bytes) as memfile:
            with memfile.open() as src:
                data = src.read(1)
                return data
    except Exception as e:
        # Not a GeoTIFF, might be PNG/WEBP/etc
        return None


def compare_tiles(tile1_bytes, tile2_bytes):
    """Compare two tiles and return difference statistics."""
    arr1 = load_geotiff_as_array(tile1_bytes)
    arr2 = load_geotiff_as_array(tile2_bytes)

    if arr1 is None or arr2 is None:
        return None

    if arr1.shape != arr2.shape:
        return None

    diff = arr1.astype(float) - arr2.astype(float)

    valid_mask = (arr1 != -9999) & (arr2 != -9999)

    if not valid_mask.any():
        return None

    valid_diff = diff[valid_mask]

    return {
        'mean_diff': float(np.mean(np.abs(valid_diff))),
        'max_diff': float(np.max(np.abs(valid_diff))),
        'std_diff': float(np.std(valid_diff)),
        'median_diff': float(np.median(np.abs(valid_diff))),
        'rmse': float(np.sqrt(np.mean(valid_diff ** 2))),
        'num_pixels': int(valid_mask.sum()),
        'identical_pixels': int((diff[valid_mask] == 0).sum()),
        'percent_identical': float((diff[valid_mask] == 0).sum() / valid_mask.sum() * 100)
    }


def compare_pmtiles_archives(reference_path, comparison_paths, zoom_level=None, sample_count=50):
    """Compare multiple pmtiles archives against a reference."""

    print(f"\n{'='*80}")
    print(f"PMTiles Comparison Analysis")
    print(f"{'='*80}\n")

    print(f"Reference: {reference_path}")
    for i, path in enumerate(comparison_paths, 1):
        print(f"Compare #{i}: {path}")
    print()

    print("Scanning reference pmtiles for tiles...")
    ref_tiles = sample_tiles_from_pmtiles(reference_path, zoom_level, sample_count)
    print(f"Found {len(ref_tiles)} tiles to compare")

    if len(ref_tiles) == 0:
        print("Error: No tiles found in reference pmtiles")
        return

    if zoom_level:
        print(f"Filtering to zoom level: {zoom_level}")
    print()

    results = defaultdict(lambda: defaultdict(list))
    comparison_counts = defaultdict(int)
    skipped_not_geotiff = 0
    skipped_missing = 0

    start_time = time.time()

    for idx, (z, x, y) in enumerate(ref_tiles):
        if (idx + 1) % 5 == 0 or idx == 0:
            comparisons_made = sum(comparison_counts.values())
            elapsed = time.time() - start_time
            rate = (idx + 1) / elapsed if elapsed > 0 else 0
            eta_seconds = (len(ref_tiles) - idx - 1) / rate if rate > 0 else 0
            eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s" if eta_seconds > 0 else "calculating..."

            print(f"[{idx + 1}/{len(ref_tiles)}] {z}/{x}/{y} | compared: {comparisons_made}, skipped: {skipped_not_geotiff + skipped_missing} | rate: {rate:.1f} tiles/s | ETA: {eta_str}")

        ref_tile_data = extract_tile(reference_path, z, x, y)
        if ref_tile_data is None:
            skipped_missing += 1
            continue

        for cmp_idx, cmp_path in enumerate(comparison_paths):
            cmp_tile_data = extract_tile(cmp_path, z, x, y)

            if cmp_tile_data is None:
                skipped_missing += 1
                continue

            stats = compare_tiles(ref_tile_data, cmp_tile_data)
            if stats:
                results[cmp_idx][f'{z}/{x}/{y}'] = stats
                comparison_counts[cmp_idx] += 1
            else:
                skipped_not_geotiff += 1

    elapsed_total = time.time() - start_time
    print(f"\nProcessing complete! ({int(elapsed_total)}s total)")
    print(f"  Total tiles checked: {len(ref_tiles)}")
    print(f"  Successful comparisons: {sum(comparison_counts.values())}")
    print(f"  Skipped (not GeoTIFF): {skipped_not_geotiff}")
    print(f"  Skipped (missing tile): {skipped_missing}")
    print()

    for cmp_idx, tile_stats in results.items():
        print(f"\n{'='*80}")
        print(f"Comparison #{cmp_idx + 1}: {comparison_paths[cmp_idx]}")
        print(f"{'='*80}\n")

        if not tile_stats:
            print("No matching tiles found or no GeoTIFF tiles to compare")
            continue

        all_mean_diffs = [s['mean_diff'] for s in tile_stats.values()]
        all_max_diffs = [s['max_diff'] for s in tile_stats.values()]
        all_rmse = [s['rmse'] for s in tile_stats.values()]
        all_pct_identical = [s['percent_identical'] for s in tile_stats.values()]

        print(f"Tiles compared: {len(tile_stats)}")
        print(f"\nMean Absolute Difference (across all tiles):")
        print(f"  Average: {np.mean(all_mean_diffs):.4f}")
        print(f"  Min:     {np.min(all_mean_diffs):.4f}")
        print(f"  Max:     {np.max(all_mean_diffs):.4f}")

        print(f"\nMax Absolute Difference (across all tiles):")
        print(f"  Average: {np.mean(all_max_diffs):.4f}")
        print(f"  Min:     {np.min(all_max_diffs):.4f}")
        print(f"  Max:     {np.max(all_max_diffs):.4f}")

        print(f"\nRMSE (across all tiles):")
        print(f"  Average: {np.mean(all_rmse):.4f}")
        print(f"  Min:     {np.min(all_rmse):.4f}")
        print(f"  Max:     {np.max(all_rmse):.4f}")

        print(f"\nPercent Identical Pixels:")
        print(f"  Average: {np.mean(all_pct_identical):.2f}%")
        print(f"  Min:     {np.min(all_pct_identical):.2f}%")
        print(f"  Max:     {np.max(all_pct_identical):.2f}%")

        worst_tiles = sorted(tile_stats.items(),
                           key=lambda x: x[1]['mean_diff'],
                           reverse=True)[:5]

        print(f"\nTop 5 tiles with largest differences:")
        for tile_id, stats in worst_tiles:
            print(f"  {tile_id}: mean={stats['mean_diff']:.4f}, max={stats['max_diff']:.4f}, identical={stats['percent_identical']:.1f}%")

    print(f"\n{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Compare pmtiles archives to analyze differences',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('reference', help='Reference pmtiles file')
    parser.add_argument('comparison', nargs='+', help='Pmtiles file(s) to compare against reference')
    parser.add_argument('--zoom', type=int, help='Specific zoom level to analyze')
    parser.add_argument('--sample-count', type=int, default=50,
                       help='Number of tiles to sample (default: 50)')

    args = parser.parse_args()

    ref_path = Path(args.reference)
    if not ref_path.exists():
        print(f"Error: Reference file not found: {ref_path}")
        sys.exit(1)

    cmp_paths = []
    for path_str in args.comparison:
        path = Path(path_str)
        if not path.exists():
            print(f"Error: Comparison file not found: {path}")
            sys.exit(1)
        cmp_paths.append(path)

    compare_pmtiles_archives(
        str(ref_path),
        [str(p) for p in cmp_paths],
        zoom_level=args.zoom,
        sample_count=args.sample_count
    )


if __name__ == '__main__':
    main()
