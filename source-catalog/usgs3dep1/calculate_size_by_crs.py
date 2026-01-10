#!/usr/bin/env python3

import json
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from osgeo import gdal

gdal.SetConfigOption('CPL_VSIL_CURL_ALLOWED_EXTENSIONS', '.tif')
gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN', 'EMPTY_DIR')

FILE_LIST = Path(__file__).parent / "file_list.txt"
OUTPUT_JSON = Path(__file__).parent / "files_by_crs.json"
FAILED_JSON = Path(__file__).parent / "files_failed.json"
NO_CRS_JSON = Path(__file__).parent / "files_no_crs.json"

def get_file_metadata(url):
    try:
        response = requests.head(url, timeout=10, allow_redirects=True)
        response.raise_for_status()
        size = int(response.headers.get('Content-Length', 0))

        crs = None
        try:
            vsicurl_url = f"/vsicurl/{url}"
            ds = gdal.Open(vsicurl_url)
            if ds:
                srs = ds.GetSpatialRef()
                if srs:
                    auth_name = srs.GetAuthorityName(None)
                    auth_code = srs.GetAuthorityCode(None)
                    if auth_name and auth_code:
                        crs = f"{auth_name}:{auth_code}"
                ds = None
        except Exception:
            pass

        return url, size, crs, None
    except Exception as e:
        return url, 0, None, str(e)

def main():
    print("Reading file list...")
    with open(FILE_LIST) as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"Found {len(urls)} files to check\n")

    crs_groups = defaultdict(lambda: {"files": [], "total_size": 0})
    failed_files = []
    no_crs_files = []
    total_size = 0
    successful = 0
    failed = 0
    no_crs = 0

    print("Fetching file metadata (size + CRS) - this may take a while...\n")

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(get_file_metadata, url): url for url in urls}

        for i, future in enumerate(as_completed(futures), 1):
            url, size, crs, error = future.result()

            if error:
                failed += 1
                failed_files.append({"url": url, "error": error})
                if failed <= 10:
                    print(f"  Error fetching {url}: {error}")
            else:
                successful += 1
                total_size += size

                if crs:
                    crs_groups[crs]["files"].append(url)
                    crs_groups[crs]["total_size"] += size
                else:
                    no_crs += 1
                    no_crs_files.append({"url": url, "size": size})

            if i % 100 == 0:
                current_gb = total_size / (1024**3)
                print(f"Progress: {i}/{len(urls)} files checked ({successful} successful, {failed} failed)")
                print(f"  Current total: {current_gb:.2f} GB\n")

    output_data = {}
    for crs, data in sorted(crs_groups.items()):
        output_data[crs] = {
            "file_count": len(data["files"]),
            "bytes": data["total_size"],
            "GiB": data["total_size"] / (1024**3),
            "files": sorted(data["files"])
        }

    with open(OUTPUT_JSON, 'w') as f:
        json.dump(output_data, f, indent=2)

    # Save failed files
    if failed_files:
        failed_output = {
            "count": len(failed_files),
            "files": failed_files
        }
        with open(FAILED_JSON, 'w') as f:
            json.dump(failed_output, f, indent=2)

    # Save files with no CRS
    if no_crs_files:
        no_crs_output = {
            "count": len(no_crs_files),
            "total_bytes": sum(f["size"] for f in no_crs_files),
            "total_GiB": sum(f["size"] for f in no_crs_files) / (1024**3),
            "files": no_crs_files
        }
        with open(NO_CRS_JSON, 'w') as f:
            json.dump(no_crs_output, f, indent=2)

    total_gb = total_size / (1024**3)
    total_tb = total_size / (1024**4)

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total files:       {len(urls):,}")
    print(f"Successful:        {successful:,}")
    print(f"Failed:            {failed:,}")
    print(f"No CRS detected:   {no_crs:,}")
    print(f"Unique CRS found:  {len(crs_groups):,}")
    print(f"Total size:        {total_size:,} bytes")
    print(f"                   {total_gb:.2f} GB")
    print(f"                   {total_tb:.2f} TB")
    print(f"\nResults saved to: {OUTPUT_JSON}")

    if failed_files:
        print(f"Failed files saved to: {FAILED_JSON}")

    if no_crs_files:
        print(f"No-CRS files saved to: {NO_CRS_JSON}")

    print("\nCRS breakdown:")
    for crs, data in sorted(output_data.items(), key=lambda x: x[1]["bytes"], reverse=True):
        print(f"  {crs}: {data['file_count']} files, {data['GiB']:.2f} GiB")

    if failed_files:
        print(f"\n⚠️  {len(failed_files)} files failed - see {FAILED_JSON.name}")

    if no_crs_files:
        print(f"⚠️  {len(no_crs_files)} files have no CRS - see {NO_CRS_JSON.name}")

    print("="*60)

if __name__ == "__main__":
    main()
