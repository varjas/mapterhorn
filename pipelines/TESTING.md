# Testing Data Sources in Mapterhorn

Quick guide for testing new data source pipelines.

## Quick Start

### 1. Run Pipeline Test
```bash
cd mapterhorn/pipelines
uv run python test_source.py <source_name>
```

Example:
```bash
uv run python test_source.py au5
```

**What it does:**
- Runs the source's Justfile (source prep: download, unzip, slice, COG, bounds, polygonize)
- Runs standard aggregation pipeline
- Runs downsampling pipeline
- Creates tile index
- Collects metrics and generates report
- Shows real-time progress and any errors

### 2. View Results

After completion, you get three ways to view your data:

**A. Tile Index (Quick Overview)**
- Open: `tile-index.html` in browser
- Shows all tiles with previews, bounds, and status
- Click "Open Viewer" button to auto-zoom to your data

**B. Preview Images (Quick Validation)**
- Location: `previews/` directory
- Hillshade PNG images for each tile
- Check these to verify terrain looks correct

**C. Interactive Viewer (Full Exploration)**
- Start server: `uv run python serve.py`
- Open: `http://localhost:8000/index.html`
- Click tiles in index to jump to locations
- Or use auto-centered link from tile-index.html

## Pipeline Management

### Complete Reset (Recommended for Testing)
Delete ALL pipeline data for a completely fresh run:
```bash
uv run python clean_pipeline.py <source_name> --reset-all
```

This removes:
- aggregation-store/ (all planning data)
- pmtiles-store/ (all generated tiles)
- source-store/<source>/ (processed source files)
- polygon-store/<source>.gpkg (coverage polygons)
- tar-store/<source>.tar (source tarballs)
- previews/ (preview images)
- Pipeline state (completion tracking)

**Use this when:**
- Testing a new file_list.txt
- Old data keeps appearing in results
- Want to verify pipeline from scratch

### Partial Clean
Remove only processing outputs (keeps source files):
```bash
uv run python clean_pipeline.py <source_name>
```

Include source downloads:
```bash
uv run python clean_pipeline.py <source_name> --full
```

Skip confirmation:
```bash
uv run python clean_pipeline.py <source_name> --reset-all --force
```

## Understanding Pipeline State

### Why Old Data Appears
The pipeline uses the **latest aggregation ID** (lexicographically sorted directory name).

Old aggregation-store directories can interfere because:
- aggregation_covering.py generates new IDs but doesn't delete old ones
- aggregation_run.py processes the latest ID
- PMTiles from multiple runs accumulate

**Solution:** Use `--reset-all` to delete everything before testing.

### State Storage
- **Location:** `.pipeline_state.db` (SQLite)
- **Contains:** Stage completion, metrics, bounds, timing
- **Cleared by:** `clean_pipeline.py --reset-all`

## Stage Output Reference

### Source Prep
- **Output:** `source-store/<source>/`
- **Files:** Downloaded TIFs, `bounds.csv`, coverage polygons
- **Metrics:** File count, total size, geographic bounds

### Aggregation
- **Output:** `pmtiles-store/`, `aggregation-store/`
- **Files:** PMTiles archives, CSV work plans, `.done` markers
- **Metrics:** Tile count, max zoom, sources used, total size

### Downsampling
- **Output:** Additional PMTiles in `pmtiles-store/`
- **Files:** Overview tiles for zoom levels below source data
- **Metrics:** Overview tile count, zoom levels created

### Index
- **Output:** `index.pmtiles`, `tile-coverage.geojson`, `tile-index.html`
- **Files:** Tile lookup index, coverage overlay, browser interface
- **Metrics:** Feature count, index size

## How It Works

### test_source.py Design
The test script **does not reimplement** pipeline logic. Instead, it:

1. **Runs your source's Justfile** as-is (for source preparation)
2. **Calls existing pipeline scripts** (aggregation_covering.py, aggregation_run.py, etc.)
3. **Monitors output** and captures success/failure
4. **Collects metrics** after each stage completes

**Why this approach:**
- Each source's Justfile can have custom configuration
- No need to keep test script in sync with pipeline changes
- The Justfile is the source of truth
- Changes to pipeline scripts automatically work in tests

### What Gets Cached
Between runs, these persist unless cleaned:
- `aggregation-store/` - Planning CSVs and .done markers
- `pmtiles-store/` - Generated terrain tiles
- `source-store/<source>/` - Processed source TIFs
- `tar-store/<source>.tar` - Packaged source data

**Problem:** aggregation_covering.py creates NEW directories each run but doesn't clean old ones.
**Solution:** Use `--reset-all` before testing to ensure clean slate.

## Tips & Tricks

### Finding Your Data Quickly
1. Open `tile-index.html` first
2. Click the big green "Open Viewer" button
3. Map auto-centers on your processed tiles
4. Use table rows to jump between tiles

### Validating Processing
1. Check that tile-index.html shows your expected tiles
2. Verify tile coordinates and bounds match your source data
3. Look at preview images if generated
4. Confirm in interactive viewer

### Testing with Limited Data
For faster iteration, edit your source's `file_list.txt`:
```bash
# In source-catalog/<source>/file_list.txt
# Comment out all but 1-2 files for testing
https://example.com/small-test-tile.zip
# https://example.com/large-dataset.zip
# https://example.com/another-large-file.zip
```

Then run:
```bash
uv run python clean_pipeline.py <source> --reset-all --force
uv run python test_source.py <source>
```

## Troubleshooting

**"Old data keeps showing up"**
- Old aggregation-store directories are interfering
- Solution: `uv run python clean_pipeline.py <source> --reset-all`
- Always use `--reset-all` when testing new file_list.txt

**"Source Preparation failed"**
- Check the Justfile commands are correct
- Verify file_list.txt URLs are valid
- Check disk space for downloads
- Look at error messages from wget/GDAL commands

**"Aggregation failed"**
- Ensure source-store/<source>/ has .tif files
- Ensure bounds.csv exists and has data rows
- Check aggregation-store for error messages

**"Blank map in viewer"**
- Open tile-index.html first and use "Open Viewer" button
- Verify PMTiles files exist in pmtiles-store/
- Check that aggregation actually completed

**"Can't find source"**
- Source must be in `source-catalog/<name>/`
- Must have `Justfile` in the source directory
- Check source name spelling matches directory name

**"bounds.csv has no data rows"**
- Source files weren't processed (check source_prep output)
- No .tif files in source-store/<source>/
- source_bounds.py may have failed

## Files Created

| File | Purpose |
|------|---------|
| `.pipeline_state.db` | Stage completion tracking (SQLite) |
| `pipeline-report-<source>-<timestamp>.html` | Summary report with metrics and links |
| `tile-index.html` | Browsable tile table with auto-zoom button |
| `tile-coverage.geojson` | Coverage overlay for map viewer |
| `index.pmtiles` | Tile lookup index |
| `source-store/<source>/bounds.csv` | Source file bounds in Web Mercator |
| `aggregation-store/<id>/*.csv` | Aggregation planning data |
| `pmtiles-store/*.pmtiles` | Generated terrain tiles |

## Running Pipeline Stages Manually

If you need to run individual stages:
```bash
cd mapterhorn/pipelines

# Source prep for a specific source
just -f ../source-catalog/au5/Justfile

# Standard aggregation (processes all sources in source-store)
uv run python aggregation_covering.py
uv run python aggregation_run.py

# Downsampling
uv run python downsampling_covering.py
uv run python downsampling_run.py

# Index
uv run python create_index.py
uv run python create_tile_index.py
```

**Note:** Manual execution bypasses state tracking and reporting.
