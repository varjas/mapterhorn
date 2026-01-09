# USGS 3DEP 1-meter DEM

This source provides 1-meter resolution Digital Elevation Model (DEM) data from the U.S. Geological Survey's 3D Elevation Program (3DEP). The data is derived from high-quality lidar point clouds and covers the United States and territories.

## Data Coverage

- **Total Products**: ~110,000+ individual GeoTIFF files
- **Coverage**: Conterminous United States, Alaska, Hawaii, and island territories
- **Resolution**: 1 meter
- **Source**: USGS 3DEP lidar-derived DEMs
- **Format**: GeoTIFF
- **License**: Public Domain (U.S. Government Work)

## Generating File Lists

### Full Dataset (All USA)

To generate the complete file list for the entire United States (WARNING: this will take 10-15 minutes due to pagination through 110k+ files):

```bash
cd pipelines
just ../source-catalog/usgs3dep1m/ fetch-urls
```

This will create `file_list.txt` with all available 1m DEM files.

### Limited by Geographic Area

You can limit the file list to specific geographic areas using several methods:

#### By Bounding Box

```bash
# California region
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -124.5,32.5,-114.1,42.0 \
    --output ../source-catalog/usgs3dep1m-california/file_list.txt

# Pacific Northwest (WA, OR)
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -125.0,42.0,-116.0,49.0 \
    --output ../source-catalog/usgs3dep1m-pnw/file_list.txt

# Northeast (New England + NY)
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -80.0,40.0,-66.0,47.5 \
    --output ../source-catalog/usgs3dep1m-northeast/file_list.txt
```

#### By State

```bash
# Single state (e.g., Colorado)
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --poly-type state --poly-code CO \
    --output ../source-catalog/usgs3dep1m-colorado/file_list.txt

# Multiple states - run separately and combine
for state in WA OR CA; do
    uv run python usgs_tnm_fetch_urls.py --dataset 1m \
        --poly-type state --poly-code $state \
        --output ../source-catalog/usgs3dep1m-westcoast/file_list_${state}.txt
done
cat ../source-catalog/usgs3dep1m-westcoast/file_list_*.txt > \
    ../source-catalog/usgs3dep1m-westcoast/file_list.txt
```

#### Limited Sample for Testing

For testing the pipeline with just a few files:

```bash
# Get first 100 files
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --max 100 \
    --output ../source-catalog/usgs3dep1m-test/file_list.txt

# Get files for a small area (e.g., around Denver, CO)
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -105.5,39.5,-104.5,40.0 \
    --output ../source-catalog/usgs3dep1m-denver/file_list.txt
```

## Processing the Data

Once you have generated a `file_list.txt`, you can process the source using the standard Mapterhorn pipeline:

```bash
cd pipelines

# Download the files (this will take a long time for the full dataset!)
uv run python source_download.py usgs3dep1m

# Or for a regional source:
uv run python source_download.py usgs3dep1m-california

# Continue with the pipeline
uv run python source_bounds.py usgs3dep1m
uv run python source_polygonize.py usgs3dep1m 32
uv run python source_create_tarball.py usgs3dep1m

# Or use the Justfile
just ../source-catalog/usgs3dep1m/
```

## Storage Requirements

The full USGS 3DEP 1m dataset is **extremely large**:

- **Number of files**: ~110,000+
- **Estimated size**: Several terabytes (varies by compression)
- **Processing time**: Days to weeks depending on hardware

For testing or regional coverage, consider:
1. Using state-level or regional subsets
2. Starting with a small bounding box
3. Using lower-resolution datasets (see `usgsned13` for 1/3 arc-second coverage)

## Additional Datasets

See the other USGS source catalogs for different resolutions and coverage:

- `usgsned13` - NED 1/3 arc-second (~10m resolution, complete USA coverage)
- `usgsned1` - NED 1 arc-second (~30m resolution, complete USA coverage)
- `usgsifsar` - Alaska IfSAR 5m DEM

## File Naming Convention

USGS 1m DEM files follow this pattern:
```
USGS_1M_{zoom}_{tile_x}y{tile_y}_{project_name}.tif
```

Example:
```
USGS_1M_10_x37y473_OR_SouthCoast_2019_A19.tif
```

- `zoom`: Tile zoom level (typically 10)
- `tile_x`, `tile_y`: Tile coordinates in some tiling scheme
- `project_name`: Source lidar project identifier
