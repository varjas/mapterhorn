# Plan: Incremental Source Pipeline Script for Mapterhorn

## Overview
Create `source_incremental.py` - a variation of `test_source.py` that processes source files one at a time, minimizing disk usage by deleting intermediate files while retaining only necessary outputs for final merge steps.

## Key Design Principles

1. **Process files individually** - Download, process, cleanup one file at a time
2. **Minimize disk footprint** - Delete source/intermediate files immediately after processing
3. **Retain merge prerequisites** - Keep only COG outputs + individual polygons for final merge
4. **Resume capability** - Track which files have been processed to support interruption/resume

## Pipeline Stages

### Stage 1: Per-File Processing Loop
For each file in `file_list.txt`:

1. **Download single file**
   - Read URL from `source-catalog/{source}/file_list.txt`
   - Download to `source-store/{source}/`
   - Validate downloaded file

2. **Unzip (if needed)**
   - Check if file is zip/7z archive
   - Extract to temp directory
   - Delete compressed archive immediately

3. **Set CRS (if needed)**
   - Apply CRS from command-line argument
   - Creates `.bak` backup, converts, deletes backup
   - Output: COG with correct CRS

4. **Convert to COG**
   - Run `source_to_cog.py` logic on single file
   - LERC compression, 512px blocks
   - Delete input, keep output `.tif`

5. **Calculate bounds**
   - Extract bounds in EPSG:3857
   - Append single line to `source-store/{source}/bounds.csv`
   - CSV accumulates as files are processed

6. **Polygonize individual file**
   - Create mask from TIF
   - Generate `.gpkg` polygon file
   - Store in `polygon-store/{source}/{filename}.gpkg`
   - Delete mask file immediately
   - **Keep the .gpkg file** (needed for merge)

7. **Cleanup decision point**
   - **DELETE**: Downloaded source file, unzip temp dirs, .bak files
   - **KEEP**: Final COG `.tif`, individual `.gpkg` polygon

### Stage 2: Final Merge Operations
After all files processed:

1. **Polygonize merge**
   - Merges all `polygon-store/{source}/*.gpkg` files
   - Creates unified `polygon-store/{source}.gpkg`
   - Deletes individual `.gpkg` files after merge
   - Uses `source_polygonize.py::merge_source()` logic

2. **Create tarball**
   - Packages: all COG `.tif` files, `coverage.gpkg`, `bounds.csv`, metadata
   - Uses `source_create_tarball.py` logic
   - Output: `tar-store/{source}.tar` + MD5 checksum

## State Tracking

Use SQLite database to track processing state:

```sql
CREATE TABLE file_progress (
    source TEXT,
    filename TEXT,
    url TEXT,
    stage TEXT,  -- 'pending', 'downloaded', 'cog', 'bounds', 'polygonized', 'complete'
    timestamp TEXT,
    PRIMARY KEY (source, filename)
);
```

This allows:
- Resume interrupted processing
- Skip already-processed files
- Report progress

## Implementation Structure

```python
class IncrementalSourceProcessor:
    def __init__(self, source: str, crs: str = None):
        self.source = source
        self.crs = crs
        self.state_db = StateTracker(source)

    def process_single_file(self, url: str, filename: str):
        """Process one file through all stages, cleanup intermediates."""
        # Download
        # Unzip (if needed)
        # Set CRS (if needed)
        # To COG
        # Bounds (append to CSV)
        # Polygonize (keep .gpkg)
        # Cleanup (delete everything except COG + .gpkg)

    def run_file_loop(self):
        """Main loop: process each file in file_list.txt."""
        urls = self.load_file_list()
        for url in urls:
            if not self.state_db.is_complete(url):
                self.process_single_file(url)
                self.state_db.mark_complete(url)

    def finalize(self):
        """Merge polygons, create tarball."""
        self.merge_polygons()
        self.create_tarball()
```

## Command-Line Interface

```bash
# Process entire source incrementally
python source_incremental.py canada_source --crs EPSG:3978

# Resume interrupted processing
python source_incremental.py canada_source --crs EPSG:3978 --resume

# Only run finalization (if file processing already done)
python source_incremental.py canada_source --finalize-only
```

## Disk Usage Comparison

**Traditional pipeline:**
- Source files: 100 GB
- Unzipped files: 150 GB
- Intermediate files: 50 GB
- Final COG files: 80 GB
- **Peak usage: ~300 GB**

**Incremental pipeline:**
- Processing 1 file at a time: ~2-5 GB temporary
- Accumulated COG outputs: 80 GB
- Individual polygons: ~1 GB
- **Peak usage: ~85 GB**

## Error Handling

- Each stage validates outputs before deleting inputs
- State tracking allows resume from last successful file
- Failed file processing logs error but continues to next file
- Final report shows success/failure per file

## Dependencies

Reuse existing utilities:
- `utils.py` - Command execution, folder creation
- Individual pipeline scripts' core functions
- `source_download.py::download_file()`
- `source_to_cog.py::to_cog()`
- `source_bounds.py` - bounds calculation logic
- `source_polygonize.py::polygonize_tif()` and `merge_source()`

## Output Artifacts

Final deliverables (same as traditional pipeline):
1. `tar-store/{source}.tar` - Complete source tarball
2. `tar-store/{source}.tar.md5` - Checksum
3. Inside tarball:
   - `files/*.tif` - All COG files
   - `coverage.gpkg` - Merged polygon coverage
   - `bounds.csv` - Bounds for all files
   - `metadata.json` - Source metadata
   - `LICENSE.pdf` - License info

---

This incremental approach allows processing large datasets with limited disk space, while producing identical outputs to the standard pipeline.
