# USGS NED 1/3 Arc-Second DEM

The National Elevation Dataset (NED) provides seamless elevation coverage of the United States at 1/3 arc-second resolution (approximately 10 meters).

## Data Coverage

- **Resolution**: 1/3 arc-second (~10 meters)
- **Coverage**: Complete coverage of the United States, Alaska, Hawaii, and territories
- **Format**: GeoTIFF
- **License**: Public Domain (U.S. Government Work)

## Why Use NED 1/3 Arc-Second?

This dataset is ideal when:
- You need **complete USA coverage** without gaps
- 10m resolution is sufficient for your application
- You want a manageable dataset size compared to 1m DEM
- You need consistent coverage across state boundaries

The 1m DEM dataset has better resolution but spotty coverage (only where lidar surveys have been conducted).

## Generating File Lists

### Full USA Coverage

```bash
cd pipelines
just ../source-catalog/usgsned13/ fetch-urls
```

### Regional Coverage

```bash
# Western USA
uv run python usgs_tnm_fetch_urls.py --dataset ned13 \
    --bbox -125,31,-102,49 \
    --output ../source-catalog/usgsned13-west/file_list.txt

# Eastern USA
uv run python usgs_tnm_fetch_urls.py --dataset ned13 \
    --bbox -102,24,-66,48 \
    --output ../source-catalog/usgsned13-east/file_list.txt
```

## Processing

```bash
cd pipelines
just ../source-catalog/usgsned13/
```
