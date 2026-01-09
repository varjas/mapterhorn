#!/usr/bin/env python3
"""
Helper script to generate file_list.txt with different subsets of the Australia 5m DEM data.

Usage:
    python generate_file_list.py [preset]

Presets:
    test     - Smallest files for quick testing (islands only)
    small    - Small mainland coverage (< 100 MB)
    medium   - Moderate coverage (< 500 MB)
    coastal  - Good coastal representation (~350 MB)
    full     - All available files (~14 GB)
"""

import sys

BASE_URL = "https://elevation-direct-downloads.s3-ap-southeast-2.amazonaws.com/5m-dem/national_utm_mosaics/"

# All available files with metadata
FILES = {
    'cocosislandz47': {
        'size_mb': 2.5,
        'description': 'Cocos (Keeling) Islands - small island territory',
        'region': 'islands'
    },
    'christmasislandz48': {
        'size_mb': 17,
        'description': 'Christmas Island - small island territory',
        'region': 'islands'
    },
    'waz51': {
        'size_mb': 59.4,
        'description': 'Western Australia Zone 51 - Perth coastal region',
        'region': 'wa_coast'
    },
    'ntz52': {
        'size_mb': 181.5,
        'description': 'Northern Territory Zone 52 - Darwin region',
        'region': 'nt_coast'
    },
    'qldz54': {
        'size_mb': 296,
        'description': 'Queensland Zone 54 - Cairns/Townsville coastal',
        'region': 'qld_coast'
    },
    'ntz53': {
        'size_mb': 341.1,
        'description': 'Northern Territory Zone 53',
        'region': 'nt_inland'
    },
    'waz50': {
        'size_mb': 2200,
        'description': 'Western Australia Zone 50 - large coverage',
        'region': 'wa_inland'
    },
    'nswqldz55': {
        'size_mb': 10800,
        'description': 'NSW/Queensland Zone 55 - Sydney/Brisbane (very large)',
        'region': 'east_coast'
    },
    'nswqldvicz56': {
        'size_mb': None,
        'description': 'NSW/Queensland/Victoria Zone 56 - Melbourne region',
        'region': 'southeast_coast'
    }
}

PRESETS = {
    'test': [
        'cocosislandz47',
        'christmasislandz48'
    ],
    'small': [
        'cocosislandz47',
        'christmasislandz48',
        'waz51'
    ],
    'medium': [
        'cocosislandz47',
        'christmasislandz48',
        'waz51',
        'ntz52',
        'qldz54'
    ],
    'coastal': [
        'waz51',
        'ntz52',
        'qldz54'
    ],
    'full': list(FILES.keys())
}


def generate_file_list(preset='test'):
    """Generate file list URLs for the given preset."""
    if preset not in PRESETS:
        print(f"Error: Unknown preset '{preset}'")
        print(f"Available presets: {', '.join(PRESETS.keys())}")
        return None

    files = PRESETS[preset]
    urls = [f"{BASE_URL}{f}.zip" for f in files]

    return urls


def print_preset_info(preset):
    """Print information about a preset."""
    if preset not in PRESETS:
        return

    files = PRESETS[preset]
    total_size = sum(FILES[f]['size_mb'] for f in files if FILES[f]['size_mb'] is not None)

    print(f"\nPreset: {preset}")
    print(f"Files: {len(files)}")
    print(f"Total size: ~{total_size:.1f} MB" if total_size > 0 else "Total size: unknown")
    print("\nIncluded regions:")
    for f in files:
        size_str = f"{FILES[f]['size_mb']:.1f} MB" if FILES[f]['size_mb'] else "unknown size"
        print(f"  - {FILES[f]['description']} ({size_str})")


def main():
    preset = 'test'
    if len(sys.argv) > 1:
        preset = sys.argv[1]

    if preset == 'help' or preset == '--help':
        print(__doc__)
        print("\nAvailable presets:")
        for p in PRESETS:
            print(f"\n{p}:")
            print_preset_info(p)
        return

    # Generate URLs
    urls = generate_file_list(preset)
    if not urls:
        return

    # Print info
    print_preset_info(preset)

    # Write to file_list.txt
    output_file = 'file_list.txt'
    with open(output_file, 'w') as f:
        for url in urls:
            f.write(url + '\n')

    print(f"\n✓ Written {len(urls)} URLs to {output_file}")
    print(f"\nTo use this list:")
    print(f"  cd ../../pipelines")
    print(f"  just -f ../source-catalog/au5/Justfile")


if __name__ == '__main__':
    main()
