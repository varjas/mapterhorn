"""Clean pipeline data for a source to enable fresh re-runs."""

import argparse
import shutil
from pathlib import Path
from pipeline_state import PipelineState


def get_size_mb(path: Path) -> float:
    """Calculate total size of directory in MB."""
    if not path.exists():
        return 0.0

    if path.is_file():
        return path.stat().st_size / (1024 * 1024)

    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total / (1024 * 1024)


def clean_pipeline(source: str, full: bool = False, reset_all: bool = False, force: bool = False):
    """
    Clean pipeline data for a source.

    Args:
        source: Source name (e.g., 'au5')
        full: If True, also delete source-store data
        reset_all: If True, delete ALL pipeline data (all stores) for fresh run
        force: Skip confirmation prompts
    """
    pipelines_dir = Path(__file__).parent
    state = PipelineState(pipelines_dir / ".pipeline_state.db")

    # Paths to clean
    paths_to_clean = []

    # Reset all mode - delete everything
    if reset_all:
        print(f"\n⚠️  RESET ALL MODE - Will delete ALL pipeline data!")
        print("This removes all cached processing for a completely fresh run.\n")

        stores = [
            ("Aggregation store", pipelines_dir / "aggregation-store"),
            ("PMTiles store", pipelines_dir / "pmtiles-store"),
            ("Source store", pipelines_dir / "source-store" / source),
            ("Polygon store", pipelines_dir / "polygon-store" / f"{source}.gpkg"),
            ("Tar store", pipelines_dir / "tar-store" / f"{source}.tar"),
            ("Tar MD5", pipelines_dir / "tar-store" / f"{source}.tar.md5"),
            ("Previews", pipelines_dir / "previews"),
        ]

        for desc, path in stores:
            if path.exists():
                paths_to_clean.append((desc, path))

    else:
        # Selective cleaning mode
        # Always clean processing outputs
        aggregation_store = pipelines_dir / "aggregation-store"
        pmtiles_store = pipelines_dir / "pmtiles-store"
        previews_dir = pipelines_dir / "previews"
        polygon_store = pipelines_dir / "polygon-store" / f"{source}.gpkg"

        # Find aggregation directories for this source
        if aggregation_store.exists():
            for agg_dir in aggregation_store.iterdir():
                if not agg_dir.is_dir():
                    continue
                # Check if any CSV files reference this source
                has_source = False
                for csv_file in agg_dir.glob("*-aggregation.csv"):
                    try:
                        content = csv_file.read_text()
                        if f"source-store/{source}/" in content:
                            has_source = True
                            break
                    except Exception:
                        pass
                if has_source:
                    paths_to_clean.append(("Aggregation data", agg_dir))

        # Find PMTiles for this source (need to check aggregation CSVs to know which tiles)
        # For simplicity, we'll note that selective PMTiles deletion requires careful tracking
        # For now, we'll just report PMTiles store size
        pmtiles_size = get_size_mb(pmtiles_store) if pmtiles_store.exists() else 0

        # Clean previews
        if previews_dir.exists():
            paths_to_clean.append(("Preview images", previews_dir))

        # Clean polygon store
        if polygon_store.exists():
            paths_to_clean.append(("Coverage polygon", polygon_store))

        # Optionally clean source data
        source_store = pipelines_dir / "source-store" / source
        if full and source_store.exists():
            paths_to_clean.append(("Source data", source_store))

    # Calculate total size
    total_size = sum(get_size_mb(path) for _, path in paths_to_clean)

    # Print summary
    print(f"\n=== Clean Pipeline: {source} ===\n")

    if not paths_to_clean:
        print(f"No data found for source '{source}'")

        # Still clear state
        summary = state.get_summary(source)
        if summary["completed_stages"]:
            print(f"\nClearing state for stages: {', '.join(summary['completed_stages'])}")
            if force or input("Clear state? [y/N]: ").lower() == 'y':
                state.clear_source(source)
                print("State cleared.")

        return

    print("Items to delete:")
    for desc, path in paths_to_clean:
        size = get_size_mb(path)
        print(f"  - {desc}: {path.relative_to(pipelines_dir)} ({size:.1f} MB)")

    if not reset_all:
        pmtiles_size = get_size_mb(pipelines_dir / "pmtiles-store") if (pipelines_dir / "pmtiles-store").exists() else 0
        if pmtiles_size > 0 and not any("PMTiles" in str(p) for _, p in paths_to_clean):
            print(f"\nNote: PMTiles store ({pmtiles_size:.1f} MB) may contain tiles from this source")
            print("      Mixed-source tiles are not automatically cleaned")
            print(f"      Use --reset-all to delete everything for a fresh run")

    print(f"\nTotal size: {total_size:.1f} MB")

    # Check state
    summary = state.get_summary(source)
    if summary["completed_stages"]:
        print(f"\nCompleted stages to clear: {', '.join(summary['completed_stages'])}")

    # Confirm
    if not force:
        response = input(f"\nDelete all items listed above? [y/N]: ")
        if response.lower() != 'y':
            print("Aborted.")
            return

    # Delete
    print("\nDeleting...")
    for desc, path in paths_to_clean:
        try:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)
            print(f"  ✓ Deleted {desc}")
        except Exception as e:
            print(f"  ✗ Failed to delete {desc}: {e}")

    # Clear state
    state.clear_source(source)
    print(f"  ✓ Cleared state")

    print(f"\nCleaning complete. Run test_pipeline.py to reprocess from scratch.")


def main():
    parser = argparse.ArgumentParser(
        description="Clean pipeline data for a source to enable fresh re-runs"
    )
    parser.add_argument("source", help="Source name (e.g., 'au5')")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Also delete source-store data (downloaded files)"
    )
    parser.add_argument(
        "--reset-all",
        action="store_true",
        help="Delete ALL pipeline data (all stores) for completely fresh run"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompts"
    )

    args = parser.parse_args()
    clean_pipeline(args.source, full=args.full, reset_all=args.reset_all, force=args.force)


if __name__ == "__main__":
    main()
