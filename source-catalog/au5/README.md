# au5

Digital Elevation Model (DEM) of Australia derived from LiDAR 5 Metre Grid.

## Quick Start

Use the `generate_file_list.py` helper script to select which regions to download:

```bash
# Small test (islands only, ~20 MB)
python generate_file_list.py test

# Coastal coverage (Perth, Darwin, Queensland ~537 MB)
python generate_file_list.py coastal

# Medium coverage (~857 MB)
python generate_file_list.py medium

# All files (~14 GB)
python generate_file_list.py full

# See all options
python generate_file_list.py help
```

Then run the pipeline:
```bash
cd ../../pipelines
just -f ../source-catalog/au5/Justfile
```

This National 5 metre (bare earth) DEM was derived from 236 individual LiDAR surveys conducted between 2001 and 2015, covering an area exceeding 245,000 square kilometres. These surveys cover Australia's populated coastal zone, floodplain surveys within the Murray Darling Basin, and individual surveys of major and minor population centres.

## Coverage Area

**Geographic Extent:**
- Longitude: 114.10° to 153.68° East
- Latitude: 43.46° to 9.87° South

The data is organized by UTM zones covering:
- Cocos Island (Zone 47)
- Christmas Island (Zone 48)
- Western Australia (Zones 50-51)
- Northern Territory (Zones 52-53)
- Queensland, NSW, Victoria (Zones 54-56)

## Boundary Geometry

For detailed coverage boundaries and tile indices:
- **Elvis Platform**: https://elevation.fsdf.org.au/ - Interactive map interface showing exact coverage areas
- **MapServer REST**: https://services.ga.gov.au/gis/rest/services/DEM_LiDAR_5m_2025/MapServer - Query layer 0 for coverage polygons
- **WMS GetCapabilities**: The WMS service provides bounding box information for each coverage area

To obtain coverage shapefiles or detailed metadata indices, contact Geoscience Australia at elevation@ga.gov.au or use the Elvis platform to interactively view coverage areas.

## Data Access

The dataset is available through:
- **Direct Downloads (S3)**: https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com/5m-dem/national_utm_mosaics/ - ZIP files by UTM zone
- **WCS Server**: https://services.ga.gov.au/gis/services/DEM_LiDAR_5m_2025/MapServer/WCSServer
- **WMS Server**: https://services.ga.gov.au/gis/services/DEM_LiDAR_5m_2025/MapServer/WMSServer
- **Elvis Platform**: https://elevation.fsdf.org.au/ (cloud-based system for discovering and obtaining Australian elevation data)
- **Google Earth Engine**: Available as AU_GA_AUSTRALIA_5M_DEM

## License

© Commonwealth of Australia (Geoscience Australia) 2015

This dataset is released under the Creative Commons Attribution 4.0 International Licence (CC BY 4.0).
