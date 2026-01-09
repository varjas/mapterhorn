# USGS Data Integration - Quick Start

I've integrated USGS elevation data access into your Mapterhorn project. Here's what was added and how to use it.

## What's New

### 1. TNM API Query Script
**Location:** `pipelines/usgs_tnm_fetch_urls.py`

This script queries the USGS National Map Access API to discover and download file URLs for any USGS elevation dataset.

### 2. Pre-configured Source Catalogs

Two source catalogs are ready to use:

#### `source-catalog/usgs3dep1m/`
- **Dataset**: USGS 3DEP 1-meter DEM
- **Resolution**: 1 meter
- **Coverage**: USA (where lidar available, ~110,000 files)
- **Size**: Very large (10-20 TB for full dataset)
- **Best for**: High-resolution local/regional analysis

#### `source-catalog/usgsned13/`
- **Dataset**: National Elevation Dataset 1/3 arc-second
- **Resolution**: ~10 meters
- **Coverage**: Complete USA (~3,800 files)
- **Size**: Moderate (500-800 GB)
- **Best for**: Complete USA coverage at medium resolution

### 3. Documentation
- **Comprehensive guide**: `docs/USGS_DATA_INTEGRATION.md`
- **Individual READMEs**: In each source catalog directory

## Quick Usage Examples

### Example 1: Generate Full USA Coverage (NED 1/3 arc-second)

This is the recommended starting point - complete USA coverage, manageable size:

```bash
cd pipelines

# Generate file list (~3,800 files)
uv run python usgs_tnm_fetch_urls.py --dataset ned13 \
    --output ../source-catalog/usgsned13/file_list.txt

# Process the source (download + prepare)
just ../source-catalog/usgsned13/
```

### Example 2: Generate Regional High-Res Data (California 1m)

For high-resolution coverage of a specific region:

```bash
cd pipelines

# Create a California-specific source catalog
mkdir -p ../source-catalog/usgs3dep1m-california
cp ../source-catalog/usgs3dep1m/metadata.json ../source-catalog/usgs3dep1m-california/
cp ../source-catalog/usgs3dep1m/Justfile ../source-catalog/usgs3dep1m-california/

# Generate file list for California only
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -124.5,32.5,-114.1,42.0 \
    --output ../source-catalog/usgs3dep1m-california/file_list.txt

# Process it
just ../source-catalog/usgs3dep1m-california/
```

### Example 3: Test with Small Dataset

For testing the pipeline with just a few files:

```bash
cd pipelines

# Get just 100 files for testing
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --max 100 \
    --output ../source-catalog/usgs3dep1m-test/file_list.txt
```

### Example 4: Query by State

```bash
cd pipelines

# Get all 1m DEM data for Colorado
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --poly-type state --poly-code CO \
    --output ../source-catalog/usgs3dep1m-colorado/file_list.txt
```

## Available Datasets

Run this to see all available dataset shortcuts:

```bash
cd pipelines
uv run python usgs_tnm_fetch_urls.py --list-datasets
```

Output:
```
1m         -> Digital Elevation Model (DEM) 1 meter
ned13      -> National Elevation Dataset (NED) 1/3 arc-second
ned1       -> National Elevation Dataset (NED) 1 arc-second
ned19      -> Original Product Resolution (OPR) DEM
ifsar      -> Alaska IFSAR 5 meter DEM
```

## Key Points

### Data Sizes
- **NED 1/3" (full USA)**: ~3,800 files, 500-800 GB, 1-2 days download
- **3DEP 1m (full USA)**: ~110,000 files, 10-20 TB, weeks to download
- **3DEP 1m (single state)**: 1,000-5,000 files, 100-500 GB, hours-days

### Generating File Lists
- File list generation is **fast** (queries API only, doesn't download data)
- Full 3DEP 1m catalog takes 10-15 minutes to query (110k products)
- NED catalogs are quicker (fewer products)
- You can generate file lists without downloading anything

### Processing Strategy
1. **Start with NED 1/3"** for complete USA baseline coverage
2. **Overlay 3DEP 1m** for high-res in areas of interest
3. **Use regional subsets** to avoid downloading unnecessary data
4. **Test with small areas** before processing full datasets

### Multi-Source Approach
The Mapterhorn pipeline is designed to blend multiple sources. Recommended layer priority:

1. USGS 3DEP 1m (highest res, USA only)
2. Regional high-res (SwissALTI3D, etc.)
3. USGS NED 1/3" (medium res, USA complete)
4. COPERNICUS GLO-30 (baseline, global)

Higher resolution sources automatically overlay lower resolution ones with smooth blending.

## Next Steps

### Recommended Workflow

**For testing:**
```bash
# Generate a small test dataset
cd pipelines
uv run python usgs_tnm_fetch_urls.py --dataset 1m --max 50 \
    --output ../source-catalog/usgs-test/file_list.txt
```

**For production - Option A (Complete USA baseline):**
```bash
# Use NED 1/3 arc-second for full coverage
cd pipelines
uv run python usgs_tnm_fetch_urls.py --dataset ned13 \
    --output ../source-catalog/usgsned13/file_list.txt
just ../source-catalog/usgsned13/
```

**For production - Option B (Regional high-res):**
```bash
# Pick regions of interest and use 1m DEM
# Example: Western USA
cd pipelines
uv run python usgs_tnm_fetch_urls.py --dataset 1m \
    --bbox -125,31,-102,49 \
    --output ../source-catalog/usgs3dep1m-west/file_list.txt
```

## Troubleshooting

**Q: The file list generation is taking a long time**
A: For the full 3DEP 1m dataset (110k+ products), this is normal. It takes 10-15 minutes due to API pagination. Consider using a regional subset instead.

**Q: I want to test without downloading huge amounts of data**
A: Use `--max 100` to limit results, or use a small bounding box like `--bbox -105.5,39.5,-104.5,40.0`

**Q: How do I know what areas have 1m coverage?**
A: The 3DEP 1m dataset has spotty coverage (only where lidar surveys were conducted). Generate the file list first, then check the bounding boxes. Populated areas and coasts generally have good coverage.

**Q: Can I download just specific tiles without the full dataset?**
A: Yes! Generate the file list, then manually edit `file_list.txt` to include only the URLs you want. The pipeline will only download what's in that file.

## More Information

- **Full documentation**: `docs/USGS_DATA_INTEGRATION.md`
- **Script help**: `uv run python usgs_tnm_fetch_urls.py --help`
- **Source catalog READMEs**: In each `source-catalog/usgs*/` directory

## License

All USGS data is in the **Public Domain** (U.S. Government Work) and can be used without restrictions.
