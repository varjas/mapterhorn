import os
import utils
import sys


def download_from_internet(source):
    file_list_path = f'../source-catalog/{source}/file_list.txt'

    if not os.path.exists(file_list_path):
        raise FileNotFoundError(f"File list not found: {file_list_path}")

    urls = []
    with open(file_list_path) as f:
        urls = [l.strip() for l in f.readlines()]
    j = 0
    for url in urls:
        j += 1
        if j % 100 == 0:
            print(f"downloaded {j} / {len(urls)}")

        command = f'cd source-store/{source} && wget --no-verbose --continue "{url}"'
        utils.run_command(command, silent=False)


def main():
    if len(sys.argv) < 2:
        print("ERROR: source argument missing")
        print("Usage: uv run python source_download.py <source_name>")
        sys.exit(1)

    source = sys.argv[1]
    print(f"Downloading {source}...\n")

    utils.create_folder(f"source-store/{source}/")

    try:
        download_from_internet(source)
        print(f"\n✓ SUCCESS: All files for '{source}' downloaded successfully")
    except Exception as e:
        print(f"\n✗ FAILED: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
