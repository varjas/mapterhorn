"""Simple test wrapper for data source pipelines - runs actual Justfiles and reports status."""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from pipeline_state import PipelineState
from pipeline_reporter import StageReporter


class SourceTester:
    """Run and monitor pipeline for a data source."""

    def __init__(self, source: str):
        self.source = source
        self.pipelines_dir = Path(__file__).parent
        self.source_catalog_dir = self.pipelines_dir.parent / "source-catalog" / source
        self.state = PipelineState(self.pipelines_dir / ".pipeline_state.db")
        self.reporter = StageReporter(source, self.pipelines_dir)

    def check_source_exists(self) -> bool:
        """Verify the source catalog directory exists."""
        if not self.source_catalog_dir.exists():
            print(f"Error: Source '{self.source}' not found at {self.source_catalog_dir}")
            print(f"\nAvailable sources:")
            catalog_dir = self.pipelines_dir.parent / "source-catalog"
            for src_dir in sorted(catalog_dir.iterdir()):
                if src_dir.is_dir() and (src_dir / "Justfile").exists():
                    print(f"  - {src_dir.name}")
            return False
        return True

    def load_metadata(self):
        """Load source metadata if available."""
        metadata_file = self.source_catalog_dir / "metadata.json"
        if metadata_file.exists():
            try:
                return json.loads(metadata_file.read_text())
            except Exception as e:
                print(f"Warning: Failed to load metadata.json: {e}")
        return None

    def run_command(self, name: str, cmd: list, cwd: Path = None) -> tuple[bool, float]:
        """Run a command and return (success, duration)."""
        print(f"\n{'─'*70}")
        print(f"Running: {name}")
        print(f"Command: {' '.join(cmd)}")
        print(f"{'─'*70}\n")

        start_time = time.time()
        result = subprocess.run(cmd, cwd=cwd or self.pipelines_dir)
        duration = time.time() - start_time

        success = result.returncode == 0

        if success:
            print(f"\n✓ {name} completed ({duration:.1f}s)")
        else:
            print(f"\n✗ {name} failed (exit code {result.returncode})")

        return success, duration

    def run_source_prep(self) -> tuple[bool, float]:
        """Run source-specific Justfile."""
        justfile = self.source_catalog_dir / "Justfile"
        if not justfile.exists():
            print(f"No Justfile found for {self.source}")
            return False, 0

        return self.run_command(
            "Source Preparation",
            ["just", "-f", str(justfile)],
            cwd=self.pipelines_dir  # Run from pipelines dir as Justfiles expect
        )

    def run_aggregation(self) -> tuple[bool, float]:
        """Run aggregation covering and execution."""
        print(f"\n{'─'*70}")
        print(f"Running: Aggregation")
        print(f"{'─'*70}\n")

        start_time = time.time()

        # Covering
        print("Planning aggregation tiles...")
        result = subprocess.run(
            ["uv", "run", "python", "aggregation_covering.py"],
            cwd=self.pipelines_dir
        )
        if result.returncode != 0:
            print("\n✗ Aggregation covering failed")
            return False, time.time() - start_time

        # Run
        print("\nExecuting aggregation...")
        result = subprocess.run(
            ["uv", "run", "python", "aggregation_run.py"],
            cwd=self.pipelines_dir
        )

        duration = time.time() - start_time
        success = result.returncode == 0

        if success:
            print(f"\n✓ Aggregation completed ({duration:.1f}s)")
        else:
            print(f"\n✗ Aggregation failed")

        return success, duration

    def run_downsampling(self) -> tuple[bool, float]:
        """Run downsampling covering and execution."""
        print(f"\n{'─'*70}")
        print(f"Running: Downsampling")
        print(f"{'─'*70}\n")

        start_time = time.time()

        # Covering
        print("Planning downsampling tiles...")
        result = subprocess.run(
            ["uv", "run", "python", "downsampling_covering.py"],
            cwd=self.pipelines_dir
        )
        if result.returncode != 0:
            print("\n✗ Downsampling covering failed")
            return False, time.time() - start_time

        # Run
        print("\nExecuting downsampling...")
        result = subprocess.run(
            ["uv", "run", "python", "downsampling_run.py"],
            cwd=self.pipelines_dir
        )

        duration = time.time() - start_time
        success = result.returncode == 0

        if success:
            print(f"\n✓ Downsampling completed ({duration:.1f}s)")
        else:
            print(f"\n✗ Downsampling failed")

        return success, duration

    def run_index(self) -> tuple[bool, float]:
        """Create tile index."""
        print(f"\n{'─'*70}")
        print(f"Running: Index Creation")
        print(f"{'─'*70}\n")

        start_time = time.time()

        result = subprocess.run(
            ["uv", "run", "python", "create_index.py"],
            cwd=self.pipelines_dir
        )
        if result.returncode != 0:
            print("\n✗ Index creation failed")
            return False, time.time() - start_time

        result = subprocess.run(
            ["uv", "run", "python", "create_tile_index.py"],
            cwd=self.pipelines_dir
        )

        duration = time.time() - start_time
        success = result.returncode == 0

        if success:
            print(f"\n✓ Index creation completed ({duration:.1f}s)")
        else:
            print(f"\n✗ Tile index creation failed")

        return success, duration

    def collect_and_save_metrics(self, stage: str, duration: float):
        """Collect metrics after a stage completes."""
        try:
            if stage == "source_prep":
                metrics = self.reporter.collect_source_prep_metrics()
            elif stage == "aggregation":
                metrics = self.reporter.collect_aggregation_metrics()
            elif stage == "downsampling":
                metrics = self.reporter.collect_downsampling_metrics()
            elif stage == "index":
                metrics = self.reporter.collect_index_metrics()
            else:
                metrics = {}

            # Save to state
            self.state.mark_stage_complete(self.source, stage, duration)
            for key, value in metrics.items():
                self.state.set_metric(self.source, stage, key, value)

            # Update bounds if available
            if stage == "source_prep" and metrics.get('bounds'):
                b = metrics['bounds']
                min_lon = b['minx'] / 111320.0
                max_lon = b['maxx'] / 111320.0
                min_lat = b['miny'] / 111320.0
                max_lat = b['maxy'] / 111320.0
                self.state.set_bounds(self.source, min_lon, min_lat, max_lon, max_lat)

            # Print report
            self.reporter.print_stage_report(stage, metrics, duration)

        except Exception as e:
            print(f"Warning: Failed to collect metrics for {stage}: {e}")

    def run(self):
        """Run the complete pipeline."""
        if not self.check_source_exists():
            return False

        print("\n" + "="*70)
        print(f"  PIPELINE TEST: {self.source.upper()}")
        print("="*70)

        metadata = self.load_metadata()
        if metadata:
            print(f"\nSource: {metadata.get('name', 'Unknown')}")
            if 'resolution' in metadata:
                print(f"Resolution: {metadata['resolution']}m")
        print()

        stages = [
            ("source_prep", "Source Preparation", self.run_source_prep),
            ("aggregation", "Aggregation", self.run_aggregation),
            ("downsampling", "Downsampling", self.run_downsampling),
            ("index", "Index", self.run_index),
        ]

        failed_stage = None
        for stage_id, stage_name, stage_func in stages:
            try:
                success, duration = stage_func()

                if success:
                    self.collect_and_save_metrics(stage_id, duration)
                else:
                    failed_stage = stage_name
                    break

            except KeyboardInterrupt:
                print(f"\n\nInterrupted by user.")
                sys.exit(1)
            except Exception as e:
                print(f"\n✗ {stage_name} failed with error: {e}")
                failed_stage = stage_name
                break

        if failed_stage:
            print(f"\n{'='*70}")
            print(f"Pipeline stopped at: {failed_stage}")
            print(f"{'='*70}\n")
            print("Check the error messages above for details.")
            print(f"To reset and try again: uv run python clean_pipeline.py {self.source} --reset-all")
            return False

        # Success!
        print(f"\n{'='*70}")
        print("  PIPELINE COMPLETE!")
        print(f"{'='*70}\n")

        # Generate report
        report_file = self.reporter.generate_html_report()
        print(f"✓ Report: {report_file.name}")

        # Show view instructions
        bounds = self.state.get_bounds(self.source)
        if bounds:
            center_lon = (bounds['min_lon'] + bounds['max_lon']) / 2
            center_lat = (bounds['min_lat'] + bounds['max_lat']) / 2
            print(f"\nView results:")
            print(f"  1. Tile index: tile-index.html")
            print(f"  2. Map viewer: index.html#map={int(12)}/{center_lat:.6f}/{center_lon:.6f}")
            print(f"\nStart server: uv run python serve.py")
        else:
            print(f"\nView results: tile-index.html")

        return True


def main():
    parser = argparse.ArgumentParser(
        description="Test pipeline for Mapterhorn data sources"
    )
    parser.add_argument("source", help="Source name (e.g., 'au5')")
    args = parser.parse_args()

    tester = SourceTester(args.source)
    success = tester.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
