#!/usr/bin/env python3
"""
Create an HTML index page showing all processed tiles with clickable map links.
Also generates a GeoJSON coverage overlay for the viewer.

Usage:
    uv run python create_tile_index.py
"""

from glob import glob
import json
import mercantile
import os
from pathlib import Path


def get_latest_aggregation_id():
    """Get the most recently modified aggregation UUID."""
    aggregation_dirs = glob('aggregation-store/*/')
    if not aggregation_dirs:
        print("No aggregation directories found in aggregation-store/")
        return None

    # Sort by modification time, most recent first
    latest = max(aggregation_dirs, key=os.path.getmtime)
    aggregation_id = latest.rstrip('/').split('/')[-1]
    return aggregation_id


def parse_tile_info(filepath):
    """Extract tile information from aggregation CSV filepath."""
    filename = Path(filepath).name

    # Parse filename: z-x-y-maxzoom-aggregation.csv
    parts = filename.replace('-aggregation.csv', '').split('-')
    if len(parts) != 4:
        return None

    z, x, y, maxzoom = [int(p) for p in parts]

    # Get geographic bounds using mercantile
    tile = mercantile.Tile(x=x, y=y, z=z)
    bounds = mercantile.bounds(tile)

    center_lon = (bounds.west + bounds.east) / 2
    center_lat = (bounds.south + bounds.north) / 2

    # Read CSV to get source files
    sources = []
    with open(filepath, 'r') as f:
        lines = f.readlines()
        for line in lines[1:]:  # Skip header
            if line.strip():
                parts = line.strip().split(',')
                if len(parts) >= 2:
                    sources.append({'source': parts[0], 'filename': parts[1]})

    # Check if processing completed
    done_file = filepath.replace('-aggregation.csv', '-aggregation.done')
    completed = os.path.exists(done_file)

    return {
        'z': z,
        'x': x,
        'y': y,
        'maxzoom': maxzoom,
        'center_lon': center_lon,
        'center_lat': center_lat,
        'bounds': {
            'west': bounds.west,
            'south': bounds.south,
            'east': bounds.east,
            'north': bounds.north
        },
        'sources': sources,
        'completed': completed,
        'filepath': filepath
    }


def create_geojson_coverage(tiles):
    """Create GeoJSON with rectangles for each tile."""
    features = []

    for tile in tiles:
        bounds = tile['bounds']

        feature = {
            "type": "Feature",
            "properties": {
                "z": tile['z'],
                "x": tile['x'],
                "y": tile['y'],
                "maxzoom": tile['maxzoom'],
                "completed": tile['completed'],
                "source_count": len(tile['sources'])
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [bounds['west'], bounds['south']],
                    [bounds['east'], bounds['south']],
                    [bounds['east'], bounds['north']],
                    [bounds['west'], bounds['north']],
                    [bounds['west'], bounds['south']]
                ]]
            }
        }
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features
    }


def create_html_index(tiles, aggregation_id):
    """Create HTML page with tile table and clickable links."""

    # Calculate statistics
    total_tiles = len(tiles)
    completed_tiles = sum(1 for t in tiles if t['completed'])

    # Calculate overall bounds for auto-zoom
    if tiles:
        min_lon = min(t['bounds']['west'] for t in tiles)
        max_lon = max(t['bounds']['east'] for t in tiles)
        min_lat = min(t['bounds']['south'] for t in tiles)
        max_lat = max(t['bounds']['north'] for t in tiles)
        center_lon = (min_lon + max_lon) / 2
        center_lat = (min_lat + max_lat) / 2
        # Calculate appropriate zoom based on bounds span
        lat_span = max_lat - min_lat
        lon_span = max_lon - min_lon
        max_span = max(lat_span, lon_span)
        # Rough zoom calculation
        if max_span > 100:
            auto_zoom = 4
        elif max_span > 50:
            auto_zoom = 5
        elif max_span > 20:
            auto_zoom = 6
        elif max_span > 10:
            auto_zoom = 7
        elif max_span > 5:
            auto_zoom = 8
        elif max_span > 2:
            auto_zoom = 9
        elif max_span > 1:
            auto_zoom = 10
        else:
            auto_zoom = 11
    else:
        center_lon = 0
        center_lat = 0
        auto_zoom = 2

    html = f"""<!DOCTYPE html>
<html lang='en'>
<head>
    <meta charset='utf-8'>
    <title>Tile Index - {aggregation_id}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            margin: 0 0 10px 0;
            color: #333;
        }}
        .stats {{
            color: #666;
            font-size: 14px;
        }}
        .stats strong {{
            color: #333;
        }}
        .viewer-button {{
            display: inline-block;
            margin-top: 15px;
            padding: 10px 20px;
            background: #4CAF50;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-weight: 600;
            transition: background 0.2s;
        }}
        .viewer-button:hover {{
            background: #45a049;
        }}
        table {{
            width: 100%;
            background: white;
            border-collapse: collapse;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-radius: 8px;
            overflow: hidden;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        th {{
            background: #f8f9fa;
            font-weight: 600;
            color: #333;
            position: sticky;
            top: 0;
        }}
        tr:hover {{
            background: #f8f9fa;
        }}
        .tile-coords {{
            font-family: monospace;
            font-size: 13px;
            color: #666;
        }}
        .geo-coords {{
            font-size: 13px;
            color: #666;
        }}
        .preview-thumbnail {{
            width: 80px;
            height: 80px;
            object-fit: cover;
            border-radius: 4px;
            border: 1px solid #ddd;
            cursor: pointer;
            transition: transform 0.2s;
        }}
        .preview-thumbnail:hover {{
            transform: scale(1.5);
            z-index: 100;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        }}
        .no-preview {{
            width: 80px;
            height: 80px;
            background: #f0f0f0;
            border-radius: 4px;
            border: 1px solid #ddd;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 11px;
            color: #999;
        }}
        .sources {{
            font-size: 12px;
            color: #666;
            max-width: 300px;
        }}
        .source-item {{
            margin-bottom: 4px;
        }}
        .source-name {{
            font-weight: 500;
            color: #0066cc;
        }}
        .status {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
        }}
        .status.completed {{
            background: #d4edda;
            color: #155724;
        }}
        .status.pending {{
            background: #fff3cd;
            color: #856404;
        }}
        .view-link {{
            display: inline-block;
            padding: 6px 12px;
            background: #0066cc;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-size: 13px;
            transition: background 0.2s;
        }}
        .view-link:hover {{
            background: #0052a3;
        }}
        .note {{
            margin-top: 20px;
            padding: 15px;
            background: white;
            border-radius: 8px;
            border-left: 4px solid #0066cc;
            font-size: 14px;
            color: #666;
        }}
        .note strong {{
            color: #333;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Tile Index</h1>
        <div class="stats">
            <strong>Aggregation ID:</strong> {aggregation_id}<br>
            <strong>Total Tiles:</strong> {total_tiles}
            (<strong>{completed_tiles}</strong> completed, <strong>{total_tiles - completed_tiles}</strong> pending)
        </div>
        <a href="index.html#map={auto_zoom}/{center_lat:.6f}/{center_lon:.6f}" class="viewer-button" target="_blank">
            🗺️ Open Viewer (Auto-centered on Data)
        </a>
    </div>

    <table>
        <thead>
            <tr>
                <th>Preview</th>
                <th>Tile Coords<br>(z/x/y)</th>
                <th>Max Zoom</th>
                <th>Center<br>(lat, lon)</th>
                <th>Bounds<br>(W, S, E, N)</th>
                <th>Sources</th>
                <th>Status</th>
                <th>Action</th>
            </tr>
        </thead>
        <tbody>
"""

    # Sort tiles by z, then x, then y
    sorted_tiles = sorted(tiles, key=lambda t: (t['z'], t['x'], t['y']))

    for tile in sorted_tiles:
        # Calculate appropriate zoom level for viewing
        # Add 2-3 zoom levels to the tile zoom to see detail
        view_zoom = min(tile['maxzoom'], tile['z'] + 3)

        # Create viewer URL with hash navigation
        viewer_url = f"index.html#map={view_zoom}/{tile['center_lat']:.6f}/{tile['center_lon']:.6f}"

        # Check for preview image
        # Preview path: previews/{z}-{x}-{y}-{child_z}.png or previews/{parent_z}-{parent_x}-{parent_y}/{z}-{x}-{y}-{child_z}.png
        preview_filename = f"{tile['z']}-{tile['x']}-{tile['y']}-{tile['maxzoom']}.png"
        preview_path = Path(f"previews/{preview_filename}")
        if not preview_path.exists():
            # Try nested path for z >= 7
            if tile['z'] >= 7:
                parent_z = 7
                parent_x = tile['x'] >> (tile['z'] - parent_z)
                parent_y = tile['y'] >> (tile['z'] - parent_z)
                preview_path = Path(f"previews/{parent_z}-{parent_x}-{parent_y}/{preview_filename}")

        if preview_path.exists():
            preview_html = f"<img src='{preview_path}' class='preview-thumbnail' alt='Tile preview' title='Click to view on map'>"
        else:
            preview_html = "<div class='no-preview'>No preview</div>"

        # Format sources list
        sources_html = ""
        for src in tile['sources']:
            sources_html += f"<div class='source-item'><span class='source-name'>{src['source']}</span>: {src['filename']}</div>"

        if not sources_html:
            sources_html = "<em>No sources</em>"

        # Status badge
        status_class = "completed" if tile['completed'] else "pending"
        status_text = "Completed" if tile['completed'] else "Pending"

        html += f"""
            <tr>
                <td>{preview_html}</td>
                <td class='tile-coords'>{tile['z']}/{tile['x']}/{tile['y']}</td>
                <td>{tile['maxzoom']}</td>
                <td class='geo-coords'>{tile['center_lat']:.4f}, {tile['center_lon']:.4f}</td>
                <td class='geo-coords' style='font-size: 11px;'>
                    {tile['bounds']['west']:.3f}, {tile['bounds']['south']:.3f}<br>
                    {tile['bounds']['east']:.3f}, {tile['bounds']['north']:.3f}
                </td>
                <td class='sources'>{sources_html}</td>
                <td><span class='status {status_class}'>{status_text}</span></td>
                <td><a href='{viewer_url}' class='view-link' target='_blank'>View</a></td>
            </tr>
"""

    html += """
        </tbody>
    </table>

    <div class="note">
        <strong>How to use:</strong> Click "View" to open the map viewer centered on that tile.
        The viewer URL includes the tile's center coordinates, so you can also bookmark or share these links.
        <br><br>
        <strong>Coverage Overlay:</strong> To see all tile boundaries on the map, add the generated
        <code>tile-coverage.geojson</code> as a layer in index.html.
    </div>
</body>
</html>
"""

    return html


def main():
    print("=== Creating Tile Index ===\n")

    # Find latest aggregation
    aggregation_id = get_latest_aggregation_id()
    if not aggregation_id:
        print("Error: No aggregation directories found.")
        print("Run aggregation_covering.py first to create aggregation tasks.")
        return

    print(f"Using aggregation ID: {aggregation_id}\n")

    # Find all aggregation CSV files
    pattern = f'aggregation-store/{aggregation_id}/*-aggregation.csv'
    csv_files = glob(pattern)

    if not csv_files:
        print(f"No aggregation CSV files found in aggregation-store/{aggregation_id}/")
        return

    print(f"Found {len(csv_files)} aggregation tasks\n")

    # Parse tile information
    tiles = []
    for filepath in csv_files:
        tile_info = parse_tile_info(filepath)
        if tile_info:
            tiles.append(tile_info)

    print(f"Parsed {len(tiles)} tiles")
    completed = sum(1 for t in tiles if t['completed'])
    print(f"  - {completed} completed")
    print(f"  - {len(tiles) - completed} pending\n")

    # Create GeoJSON coverage file
    print("Creating tile-coverage.geojson...")
    geojson = create_geojson_coverage(tiles)
    with open('tile-coverage.geojson', 'w') as f:
        json.dump(geojson, f, indent=2)
    print(f"  ✓ Created tile-coverage.geojson ({len(tiles)} features)\n")

    # Create HTML index page
    print("Creating tile-index.html...")
    html = create_html_index(tiles, aggregation_id)
    with open('tile-index.html', 'w') as f:
        f.write(html)
    print("  ✓ Created tile-index.html\n")

    print("=== Tile Index Creation Complete ===\n")
    print("To use:")
    print("  1. Open tile-index.html in your browser")
    print("  2. Click 'View' next to any tile to jump to that location")
    print("  3. Optional: Add tile-coverage.geojson as an overlay in index.html")
    print("")


if __name__ == '__main__':
    main()
