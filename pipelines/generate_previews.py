"""Generate hillshade preview images from PMTiles files."""

import argparse
import sqlite3
import struct
from pathlib import Path
from typing import List, Tuple, Optional
import numpy as np
from PIL import Image


def decode_terrarium(rgb_data: np.ndarray) -> np.ndarray:
    """
    Decode terrarium-encoded elevation from RGB values.

    Formula: elevation = (R * 256 + G + B / 256) - 32768
    """
    r = rgb_data[:, :, 0].astype(np.float32)
    g = rgb_data[:, :, 1].astype(np.float32)
    b = rgb_data[:, :, 2].astype(np.float32)

    elevation = (r * 256.0 + g + b / 256.0) - 32768.0
    return elevation


def generate_hillshade(elevation: np.ndarray, azimuth: float = 315.0, altitude: float = 45.0) -> np.ndarray:
    """
    Generate hillshade from elevation data.

    Args:
        elevation: 2D array of elevation values
        azimuth: Light direction in degrees (0-360)
        altitude: Light angle above horizon in degrees (0-90)

    Returns:
        Hillshade values in range 0-255
    """
    # Convert angles to radians
    azimuth_rad = np.radians(azimuth)
    altitude_rad = np.radians(altitude)

    # Calculate gradients
    x_grad, y_grad = np.gradient(elevation)

    # Calculate slope and aspect
    slope = np.arctan(np.sqrt(x_grad**2 + y_grad**2))
    aspect = np.arctan2(-x_grad, y_grad)

    # Calculate hillshade
    shaded = (
        np.sin(altitude_rad) * np.cos(slope) +
        np.cos(altitude_rad) * np.sin(slope) * np.cos(azimuth_rad - aspect)
    )

    # Scale to 0-255
    shaded = np.clip(shaded * 255, 0, 255).astype(np.uint8)

    return shaded


def generate_multidirectional_hillshade(elevation: np.ndarray) -> np.ndarray:
    """
    Generate multi-directional hillshade by combining 4 light directions.

    Uses the same approach as the web viewer.
    """
    azimuths = [315, 45, 135, 225]  # NW, NE, SE, SW
    hillshades = []

    for azimuth in azimuths:
        hs = generate_hillshade(elevation, azimuth=azimuth, altitude=45.0)
        hillshades.append(hs)

    # Average the hillshades
    combined = np.mean(hillshades, axis=0).astype(np.uint8)

    return combined


def find_tile_in_pmtiles(pmtiles_path: Path, z: int, x: int, y: int) -> Optional[bytes]:
    """
    Extract a single tile from a PMTiles archive.

    Args:
        pmtiles_path: Path to .pmtiles file
        z, x, y: Tile coordinates

    Returns:
        Tile data as bytes, or None if not found
    """
    try:
        # PMTiles uses SQLite format (for single-tile archives) or custom format
        # For simplicity, we'll use a basic approach that works with the aggregation output

        # Try reading as SQLite first (bundled format)
        try:
            conn = sqlite3.connect(pmtiles_path)
            cursor = conn.cursor()

            # Standard MBTiles schema
            cursor.execute(
                "SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?",
                (z, x, y)
            )
            row = cursor.fetchone()
            conn.close()

            if row:
                return row[0]
        except Exception:
            pass

        # For single-tile PMTiles (from aggregation), read directly
        # The file is just a WebP image
        return pmtiles_path.read_bytes()

    except Exception as e:
        print(f"Warning: Failed to read tile {z}/{x}/{y} from {pmtiles_path}: {e}")
        return None


def generate_preview_for_pmtiles(pmtiles_path: Path, output_path: Path, size: int = 512):
    """
    Generate a hillshade preview image from a PMTiles file.

    Args:
        pmtiles_path: Path to .pmtiles file
        output_path: Where to save preview image
        size: Output image size (will be square)
    """
    # Parse tile coordinates from filename
    # Format: {z}-{x}-{y}-{child_z}.pmtiles
    parts = pmtiles_path.stem.split("-")
    if len(parts) != 4:
        print(f"Warning: Cannot parse tile coordinates from {pmtiles_path.name}")
        return

    z, x, y, child_z = map(int, parts)

    # Read the tile data (for single-tile PMTiles, the file IS the tile)
    try:
        # Most aggregation outputs are single WebP files wrapped as PMTiles
        # Try reading directly as WebP
        img = Image.open(pmtiles_path)

        # Convert to RGB if necessary
        if img.mode != 'RGB':
            img = img.convert('RGB')

        # Convert to numpy array
        rgb_data = np.array(img)

        # Decode terrarium elevation
        elevation = decode_terrarium(rgb_data)

        # Generate multi-directional hillshade
        hillshade = generate_multidirectional_hillshade(elevation)

        # Resize if needed
        if hillshade.shape[0] != size or hillshade.shape[1] != size:
            hs_img = Image.fromarray(hillshade, mode='L')
            hs_img = hs_img.resize((size, size), Image.Resampling.LANCZOS)
            hillshade = np.array(hs_img)

        # Save preview
        output_path.parent.mkdir(parents=True, exist_ok=True)
        preview_img = Image.fromarray(hillshade, mode='L')
        preview_img.save(output_path, 'PNG', optimize=True)

        print(f"  ✓ Generated preview: {output_path.name}")

    except Exception as e:
        print(f"  ✗ Failed to generate preview for {pmtiles_path.name}: {e}")


def generate_all_previews(source: str = None, pipelines_dir: Path = None):
    """
    Generate preview images for all PMTiles files.

    Args:
        source: If specified, only generate for this source
        pipelines_dir: Pipeline directory (defaults to script location)
    """
    if pipelines_dir is None:
        pipelines_dir = Path(__file__).parent

    pmtiles_store = pipelines_dir / "pmtiles-store"
    previews_dir = pipelines_dir / "previews"

    if not pmtiles_store.exists():
        print(f"No PMTiles store found at {pmtiles_store}")
        return

    # Find all PMTiles files
    pmtiles_files = list(pmtiles_store.rglob("*.pmtiles"))

    if not pmtiles_files:
        print("No PMTiles files found")
        return

    # Filter by source if specified
    if source:
        # We need to check aggregation CSVs to determine which tiles belong to this source
        # For simplicity, we'll generate all and let the user filter
        print(f"Note: Generating previews for all tiles (source filtering not yet implemented)")

    print(f"\nGenerating previews for {len(pmtiles_files)} PMTiles files...")

    for i, pmtiles_file in enumerate(pmtiles_files, 1):
        # Create output filename
        relative_path = pmtiles_file.relative_to(pmtiles_store)
        preview_path = previews_dir / relative_path.with_suffix('.png')

        # Skip if preview already exists
        if preview_path.exists():
            continue

        print(f"[{i}/{len(pmtiles_files)}] {pmtiles_file.name}")
        generate_preview_for_pmtiles(pmtiles_file, preview_path)

    print(f"\n✓ Previews saved to: {previews_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate hillshade preview images from PMTiles files"
    )
    parser.add_argument(
        "--source",
        help="Only generate previews for this source (optional)"
    )
    parser.add_argument(
        "--size",
        type=int,
        default=512,
        help="Preview image size in pixels (default: 512)"
    )

    args = parser.parse_args()
    generate_all_previews(source=args.source)


if __name__ == "__main__":
    main()
