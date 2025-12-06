from glob import glob

import mercantile
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.writer import Writer

filepaths = glob(f'pmtiles-store/*.pmtiles') + glob(f'pmtiles-store/*/*.pmtiles')
out_filepath = 'index.pmtiles'

# Calculate bounds from high-resolution tiles only (zoom 7+)
# Low-zoom tiles are downsampled and may cover the whole world
min_lon, min_lat, max_lon, max_lat = 180, 90, -180, -90
all_tiles = []

for filepath in filepaths:
    filename = filepath.split('/')[-1]
    z, x, y, child_z = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
    if z == child_z:
        tiles = [mercantile.Tile(x=x, y=y, z=z)]
    else:
        tiles = mercantile.children(mercantile.Tile(x=x, y=y, z=z), zoom=child_z)
    all_tiles.extend(tiles)

    # Only calculate bounds from zoom 7+ tiles (actual data, not downsampled)
    if z >= 7:
        for tile in tiles:
            bounds = mercantile.bounds(tile)
            min_lon = min(min_lon, bounds.west)
            min_lat = min(min_lat, bounds.south)
            max_lon = max(max_lon, bounds.east)
            max_lat = max(max_lat, bounds.north)

# Calculate center
center_lon = (min_lon + max_lon) / 2
center_lat = (min_lat + max_lat) / 2

print(f"Calculated bounds: {min_lon}, {min_lat} to {max_lon}, {max_lat}")
print(f"Center: {center_lon}, {center_lat}")
print(f"Writing {len(filepaths)} files covering {len(all_tiles)} tiles")

with open(out_filepath, 'wb') as f:
    writer = Writer(f)

    for j, filepath in enumerate(filepaths):
        if j % 100 == 0:
            print(f'{j} / {len(filepaths)}')
        filename = filepath.split('/')[-1]
        z, x, y, child_z = [int(a) for a in filename.replace('.pmtiles', '').split('-')]
        children = None
        if z == child_z:
            children = [mercantile.Tile(x=x, y=y, z=z)]
        else:
            children = mercantile.children(mercantile.Tile(x=x, y=y, z=z), zoom=child_z)
        for child in children:
            tile_id = zxy_to_tileid(child.z, child.x, child.y)
            writer.write_tile(tile_id, filepath.encode('utf-8'))
    writer.finalize(
        {
            'tile_type': TileType.UNKNOWN,
            'tile_compression': Compression.NONE,
            'min_zoom': 0,
            'max_zoom': 17,
            'min_lon_e7': int(min_lon * 1e7),
            'min_lat_e7': int(min_lat * 1e7),
            'max_lon_e7': int(max_lon * 1e7),
            'max_lat_e7': int(max_lat * 1e7),
            'center_zoom': 4,
            'center_lon_e7': int(center_lon * 1e7),
            'center_lat_e7': int(center_lat * 1e7),
        },
        {
            'attribution': '<a href="https://github.com/mapterhorn/mapterhorn">© Mapterhorn</a>'
        },
    )