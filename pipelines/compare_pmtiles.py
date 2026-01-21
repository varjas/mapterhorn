#!/usr/bin/env python3
"""
Compare multiple pmtiles outputs to analyze differences.

This script extracts sample tiles from pmtiles archives and performs:
- Pixel-by-pixel difference analysis
- Statistical comparisons (mean, std, min, max differences)
- Histogram comparisons
"""

import argparse
import json
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from pathlib import Path
import sys
from collections import defaultdict
from pmtiles.reader import Reader
from pmtiles.tile import zxy_to_tileid


def get_tile_list(pmtiles_path, zoom_level=None, sample_count=None):
    """Get list of tiles from pmtiles archive."""
    with open(pmtiles_path, 'rb') as f:
        reader = Reader(f)

        tiles = []

        # Iterate through all entries
        for entry in reader.entries():
            tile_id = entry[0]
            z, x, y = reader.tileid_to_zxy(tile_id)

            if zoom_level is None or z == zoom_level:
                tiles.append((z, x, y))

        tiles.sort()

        if sample_count and len(tiles) > sample_count:
            step = max(1, len(tiles) // sample_count)
            tiles = tiles[::step][:sample_count]

        return tiles


def extract_tile(pmtiles_path, z, x, y):
    """Extract a single tile from pmtiles as bytes."""
    with open(pmtiles_path, 'rb') as f:
        reader = Reader(f)
        tile_id = zxy_to_tileid(z, x, y)
        tile_data = reader.get_tile(tile_id)
        return tile_data


def load_geotiff_as_array(tile_bytes):
    """Load a GeoTIFF tile from bytes as a numpy array."""
    if tile_bytes is None:
        return None

    with MemoryFile(tile_bytes) as memfile:
        with memfile.open() as src:
            data = src.read(1)
            return data


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

    ref_tiles = get_tile_list(reference_path, zoom_level, sample_count)
    print(f"Found {len(ref_tiles)} tiles to compare")

    if zoom_level:
        print(f"Filtering to zoom level: {zoom_level}")
    print()

    results = defaultdict(lambda: defaultdict(list))

    for idx, (z, x, y) in enumerate(ref_tiles):
        if (idx + 1) % 10 == 0:
            print(f"Processing tile {idx + 1}/{len(ref_tiles)}...", end='\r')

        ref_tile_data = extract_tile(reference_path, z, x, y)
        if ref_tile_data is None:
            continue

        for cmp_idx, cmp_path in enumerate(comparison_paths):
            cmp_tile_data = extract_tile(cmp_path, z, x, y)

            if cmp_tile_data is None:
                continue

            stats = compare_tiles(ref_tile_data, cmp_tile_data)
            if stats:
                results[cmp_idx][f'{z}/{x}/{y}'] = stats

    print(f"\nProcessing complete. Analyzed {len(ref_tiles)} tiles\n")

    for cmp_idx, tile_stats in results.items():
        print(f"\n{'='*80}")
        print(f"Comparison #{cmp_idx + 1}: {comparison_paths[cmp_idx]}")
        print(f"{'='*80}\n")

        if not tile_stats:
            print("No matching tiles found")
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
