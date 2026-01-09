# USGS Data Integration Guide

This guide explains how to integrate USGS elevation datasets into the Mapterhorn project using the TNM Access API.

## Overview

USGS provides several elevation datasets through The National Map (TNM):

| Dataset | Resolution | Coverage | Products | Use Case |
|---------|-----------|----------|----------|----------|
| **3DEP 1m DEM** | 1 meter | USA (where lidar available) | ~110,000 | High-res, spotty coverage |
| **NED 1/3 arc-sec** | ~10 meters | Complete USA | ~3,800 | Complete coverage, medium res |
| **NED 1 arc-sec** | ~30 meters | Complete USA | ~1,000 | Complete coverage, lower res |
| **Alaska IfSAR** | 5 meters | Alaska | Varies | Alaska-specific |

## Quick Start

### 1. Generate File List

For complete USA coverage with NED 1/3 arc-second (recommended starting point):

```bash
cd pipelines
uv run python usgs_tnm_fetch_urls.py --dataset ned13 \
    --output ../source-catalog/usgsned13/file_list.txt
```

For high-resolution 1m DEM (large dataset, may take 10-15 minutes):

```bash
cd pipelines
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --output ../source-catalog/usgs3dep1m/file_list.txt
```

### 2. Process the Source

```bash
cd pipelines
just ../source-catalog/usgsned13/
```

This will:
1. Download all files from `file_list.txt`
2. Extract bounding boxes
3. Create vector polygons
4. Create a tarball archive

## Dataset Details

### 3DEP 1-Meter DEM (`usgs3dep1m`)

**Pros:**
- Highest resolution (1m)
- Derived from quality lidar data
- Best for detailed terrain analysis

**Cons:**
- Very large dataset (~110k files, several TB)
- Incomplete coverage (only where lidar surveys conducted)
- Long processing time

**File pattern:**
```
USGS_1M_10_x37y473_OR_SouthCoast_2019_A19.tif
```

**Coverage notes:**
- Good coverage in populated areas and coasts
- Spotty in rural/remote areas
- Alaska coverage varies by region

### NED 1/3 Arc-Second (`usgsned13`)

**Pros:**
- Complete USA coverage (no gaps)
- Reasonable resolution (~10m)
- Manageable dataset size (~3,800 files)
- Fast to download and process

**Cons:**
- Lower resolution than 1m DEM
- May have some artifacts at tile boundaries

**File pattern:**
```
USGS_13_n20w156_20230522.tif
```

**Coverage notes:**
- Seamless coverage of all 50 states
- Includes Alaska, Hawaii, territories
- Regular updates with date stamps

## Regional Subsets

For testing or regional focus, you can create geographic subsets:

### By Bounding Box

```bash
# Continental USA
uv run python usgs_tnm_fetch_urls.py --dataset ned13 \
    --bbox -125,24,-66,50 \
    --output ../source-catalog/usgsned13-conus/file_list.txt

# California
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -124.5,32.5,-114.1,42.0 \
    --output ../source-catalog/usgs3dep1m-california/file_list.txt

# Pacific Northwest (WA, OR, ID)
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -125.0,42.0,-111.0,49.0 \
    --output ../source-catalog/usgs3dep1m-pnw/file_list.txt

# Rocky Mountains (CO, WY, MT)
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -116.0,36.0,-101.0,49.0 \
    --output ../source-catalog/usgs3dep1m-rockies/file_list.txt

# Northeast (New England)
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -80.0,40.0,-66.0,47.5 \
    --output ../source-catalog/usgs3dep1m-northeast/file_list.txt
```

### By State

```bash
# Single state
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --poly-type state --poly-code CO \
    --output ../source-catalog/usgs3dep1m-colorado/file_list.txt

# Common state codes: CA, CO, WA, OR, MT, WY, etc.
```

### Test Dataset

For pipeline testing (just a few files):

```bash
# Small bounding box around Denver
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -105.5,39.5,-104.5,40.0 \
    --output ../source-catalog/usgs3dep1m-test/file_list.txt

# Or just limit to first N files
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --max 100 \
    --output ../source-catalog/usgs3dep1m-test/file_list.txt
```

## Creating Custom Source Catalogs

To create a new regional source catalog:

1. **Create directory structure:**
```bash
mkdir -p source-catalog/usgs3dep1m-california
```

2. **Create `metadata.json`:**
```json
{
    "name": "USGS 3DEP 1m DEM - California",
    "website": "https://www.usgs.gov/3d-elevation-program",
    "license": "Public Domain (U.S. Government Work)",
    "producer": "U.S. Geological Survey 3D Elevation Program",
    "resolution": 1.0,
    "access_year": 2025
}
```

3. **Create `Justfile`:**
```justfile
[no-cd]
default:
    uv run python source_download.py usgs3dep1m-california
    uv run python source_bounds.py usgs3dep1m-california
    uv run python source_polygonize.py usgs3dep1m-california 32
    uv run python source_create_tarball.py usgs3dep1m-california

[no-cd]
fetch-urls:
    uv run python usgs_tnm_fetch_urls.py --dataset 1m \
        --bbox -124.5,32.5,-114.1,42.0 \
        --output ../source-catalog/usgs3dep1m-california/file_list.txt
```

4. **Generate file list:**
```bash
cd pipelines
just ../source-catalog/usgs3dep1m-california/ fetch-urls
```

5. **Process the source:**
```bash
just ../source-catalog/usgs3dep1m-california/
```

## Script Reference

### `usgs_tnm_fetch_urls.py`

**Location:** `pipelines/usgs_tnm_fetch_urls.py`

**Common options:**

```bash
# List available dataset shortcuts
uv run python usgs_tnm_fetch_urls.py --list-datasets

# Query by dataset
--dataset 1m              # 1-meter DEM
--dataset ned13           # NED 1/3 arc-second
--dataset ned1            # NED 1 arc-second

# Geographic filters
--bbox minLon,minLat,maxLon,maxLat    # Bounding box
--poly-type state --poly-code CA       # By state
--poly-type huc2 --poly-code 17        # By watershed

# Output
--output file_list.txt    # Save to file
--max 100                 # Limit number of results

# Date filters
--date-start 2020-01-01   # Only products after date
--date-end 2025-12-31     # Only products before date
```

**Full help:**
```bash
uv run python usgs_tnm_fetch_urls.py --help
```

## Data Sources

### Primary Sources

All USGS datasets are in the **Public Domain** as U.S. Government works.

**Sources:**
- **3DEP 1m DEM**: Derived from lidar point clouds
- **NED**: National Elevation Dataset (merged from multiple sources)
- **IfSAR**: Interferometric Synthetic Aperture Radar (Alaska)

**Access methods:**
1. **TNM Access API** (used by this script) - Programmatic access to product catalog
2. **AWS S3** - Files hosted at `prd-tnm.s3.amazonaws.com`
3. **Direct HTTPS** - All files downloadable via standard HTTP GET

### Data Quality

**3DEP 1m DEM:**
- Quality levels: QL0 (highest), QL1, QL2 (standard)
- Vertical accuracy: ~10-20cm RMSE for QL1/QL2
- Horizontal accuracy: Better than 1 meter

**NED:**
- Vertical accuracy: Varies by source data
- Regular updates incorporating new data
- Seamless mosaics with edge matching

## Storage and Performance

### Disk Space Requirements

| Dataset | Files | Approx Size | Download Time* |
|---------|-------|-------------|----------------|
| 3DEP 1m (full) | ~110,000 | 10-20 TB | Weeks |
| 3DEP 1m (state) | 1,000-5,000 | 100-500 GB | Hours-Days |
| NED 1/3" (full) | ~3,800 | 500-800 GB | 1-2 Days |
| NED 1" (full) | ~1,000 | 50-100 GB | Hours |

*Assuming 10-50 MB/s download speed

### Processing Time

Processing time depends on:
- Number of source files
- CPU cores available
- Disk I/O speed (SSD recommended)
- Target zoom levels

**Estimates:**
- Small state (e.g., Rhode Island): Hours
- Large state (e.g., California): Days
- Full USA: Weeks

### Optimization Tips

1. **Start small**: Test with a single state or small bbox
2. **Use regional sources**: Split large areas into manageable chunks
3. **Symlink large stores**: Put `source-store/` on HDD, keep working data on SSD
4. **Parallel downloads**: The `source_download.py` script uses wget with `--continue` for resume support
5. **Incremental updates**: Only regenerate changed regions

## Comparison with Other Sources

### USGS vs COPERNICUS GLO-30

| Feature | USGS 3DEP 1m | COPERNICUS GLO-30 |
|---------|--------------|-------------------|
| Resolution | 1 meter | 30 meters |
| Coverage | USA only | Global |
| Source | Lidar | Radar (TanDEM-X) |
| Completeness | Spotty | Complete |
| Vertical accuracy | ~10-20cm | ~2-4m |
| License | Public Domain | Open (with attribution) |

**Recommendation:**
- Use **GLO-30** for baseline global coverage
- Overlay **USGS 3DEP 1m** for high-res USA coverage
- Use **NED 1/3"** to fill gaps in 1m coverage

### Multi-Source Strategy

Recommended layer priority (highest to lowest resolution):
1. USGS 3DEP 1m DEM (1m, USA high-res areas)
2. Regional high-res sources (SwissALTI3D, etc.)
3. USGS NED 1/3 arc-second (10m, USA complete)
4. USGS NED 1 arc-second (30m, USA complete)
5. COPERNICUS GLO-30 (30m, global baseline)

The Mapterhorn aggregation pipeline will automatically blend these with smooth transitions.

## Troubleshooting

### API Rate Limiting

The script includes automatic delays between requests (0.5s). If you encounter rate limiting:
- Increase delay in the script
- Run during off-peak hours
- Contact USGS for bulk access arrangements

### Missing URLs in Response

Some products may not have downloadURL fields. The script tries multiple URL fields:
- `downloadURL`
- `downloadURLRaster`
- `urls.GeoTIFF`
- `urls.TIFF`

If many products are missing URLs, check the USGS TNM status page.

### Large File Lists

Generating the full 1m DEM file list takes 10-15 minutes due to pagination through 110k+ products. This is normal - the script will show progress as it goes.

### Download Failures

The `source_download.py` script uses wget with `--continue` flag, so interrupted downloads can be resumed by re-running the command.

## References

- **USGS 3DEP**: https://www.usgs.gov/3d-elevation-program
- **The National Map**: https://www.usgs.gov/national-map
- **TNM Access API**: https://tnmaccess.nationalmap.gov/api/v1/docs
- **3DEP Product Specification**: https://www.usgs.gov/3d-elevation-program/standard-lidar-products
- **Data Quality Levels**: https://www.usgs.gov/3d-elevation-program/lidar-quality-levels

## Support

For issues with:
- **This script**: Check the script help (`--help`) or examine the source code
- **USGS data availability**: Contact tnminfo@usgs.gov
- **Mapterhorn pipeline**: See main project documentation
