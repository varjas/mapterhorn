"""Stage reporting and metrics collection for pipeline execution."""

import csv
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple
from datetime import datetime
from pipeline_state import PipelineState


class StageReporter:
    """Collect and report metrics for pipeline stages."""

    def __init__(self, source: str, pipelines_dir: Path = None):
        self.source = source
        self.pipelines_dir = pipelines_dir or Path(__file__).parent
        self.state = PipelineState(self.pipelines_dir / ".pipeline_state.db")

    def collect_source_prep_metrics(self) -> Dict[str, Any]:
        """Collect metrics from source preparation stage."""
        source_dir = self.pipelines_dir / "source-store" / self.source
        bounds_file = source_dir / "bounds.csv"

        metrics = {
            "file_count": 0,
            "total_size_mb": 0.0,
            "bounds": None,
            "files": []
        }

        if not source_dir.exists():
            return metrics

        # Count files and size
        tif_files = list(source_dir.glob("*.tif")) + list(source_dir.glob("*.tiff"))
        metrics["file_count"] = len(tif_files)

        total_size = sum(f.stat().st_size for f in tif_files if f.is_file())
        metrics["total_size_mb"] = total_size / (1024 * 1024)

        # Read bounds
        if bounds_file.exists():
            with open(bounds_file, 'r') as f:
                reader = csv.DictReader(f)
                bounds_data = list(reader)

            if bounds_data:
                # Calculate overall bounds (columns: left, bottom, right, top)
                min_x = min(float(row['left']) for row in bounds_data)
                min_y = min(float(row['bottom']) for row in bounds_data)
                max_x = max(float(row['right']) for row in bounds_data)
                max_y = max(float(row['top']) for row in bounds_data)

                metrics["bounds"] = {
                    "minx": min_x,
                    "miny": min_y,
                    "maxx": max_x,
                    "maxy": max_y
                }

                metrics["files"] = [
                    {
                        "filename": row['filename'],
                        "width": int(row['width']),
                        "height": int(row['height'])
                    }
                    for row in bounds_data
                ]

        return metrics

    def collect_aggregation_metrics(self) -> Dict[str, Any]:
        """Collect metrics from aggregation stage."""
        aggregation_store = self.pipelines_dir / "aggregation-store"
        pmtiles_store = self.pipelines_dir / "pmtiles-store"

        metrics = {
            "tile_count": 0,
            "completed_count": 0,
            "tiles": [],
            "max_zoom": 0,
            "sources_used": set()
        }

        if not aggregation_store.exists():
            return metrics

        # Find aggregation directories for this source
        for agg_dir in aggregation_store.iterdir():
            if not agg_dir.is_dir():
                continue

            # Check aggregation CSVs
            for csv_file in agg_dir.glob("*-aggregation.csv"):
                try:
                    content = csv_file.read_text()
                    if f"source-store/{self.source}/" not in content:
                        continue

                    # Parse tile coordinates from filename
                    # Format: {z}-{x}-{y}-{child_z}-aggregation.csv
                    parts = csv_file.stem.replace("-aggregation", "").split("-")
                    if len(parts) == 4:
                        z, x, y, child_z = map(int, parts)
                        metrics["tile_count"] += 1
                        metrics["max_zoom"] = max(metrics["max_zoom"], child_z)

                        # Check if completed
                        done_file = csv_file.with_suffix(".done")
                        is_complete = done_file.exists()
                        if is_complete:
                            metrics["completed_count"] += 1

                        # Read sources used
                        with open(csv_file, 'r') as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                metrics["sources_used"].add(row['source'])

                        metrics["tiles"].append({
                            "z": z,
                            "x": x,
                            "y": y,
                            "child_z": child_z,
                            "completed": is_complete
                        })

                except Exception as e:
                    print(f"Warning: Failed to parse {csv_file}: {e}")

        metrics["sources_used"] = sorted(list(metrics["sources_used"]))

        # Count PMTiles files
        pmtiles_count = 0
        pmtiles_size = 0
        if pmtiles_store.exists():
            for pmtiles_file in pmtiles_store.rglob("*.pmtiles"):
                pmtiles_count += 1
                pmtiles_size += pmtiles_file.stat().st_size

        metrics["pmtiles_count"] = pmtiles_count
        metrics["pmtiles_size_mb"] = pmtiles_size / (1024 * 1024)

        return metrics

    def collect_downsampling_metrics(self) -> Dict[str, Any]:
        """Collect metrics from downsampling stage."""
        aggregation_store = self.pipelines_dir / "aggregation-store"

        metrics = {
            "tile_count": 0,
            "completed_count": 0,
            "zoom_levels": set()
        }

        if not aggregation_store.exists():
            return metrics

        # Find downsampling CSVs
        for agg_dir in aggregation_store.iterdir():
            if not agg_dir.is_dir():
                continue

            for csv_file in agg_dir.glob("*-downsampling.csv"):
                try:
                    # Parse tile coordinates
                    parts = csv_file.stem.replace("-downsampling", "").split("-")
                    if len(parts) == 4:
                        z, x, y, child_z = map(int, parts)
                        metrics["tile_count"] += 1
                        metrics["zoom_levels"].add(z)

                        done_file = csv_file.with_suffix(".done")
                        if done_file.exists():
                            metrics["completed_count"] += 1

                except Exception:
                    pass

        metrics["zoom_levels"] = sorted(list(metrics["zoom_levels"]))

        return metrics

    def collect_index_metrics(self) -> Dict[str, Any]:
        """Collect metrics from index creation stage."""
        index_file = self.pipelines_dir / "index.pmtiles"
        coverage_file = self.pipelines_dir / "tile-coverage.geojson"

        metrics = {
            "index_exists": index_file.exists(),
            "coverage_exists": coverage_file.exists(),
            "feature_count": 0
        }

        if coverage_file.exists():
            try:
                with open(coverage_file, 'r') as f:
                    data = json.load(f)
                    metrics["feature_count"] = len(data.get("features", []))
            except Exception:
                pass

        return metrics

    def print_stage_report(self, stage: str, metrics: Dict[str, Any], duration: float = None):
        """Print a formatted report for a stage."""
        print(f"\n{'='*60}")
        print(f"Stage: {stage.upper()}")
        print(f"{'='*60}")

        if duration is not None:
            print(f"Duration: {duration:.1f}s")

        if stage == "source_prep":
            print(f"Files: {metrics.get('file_count', 0)}")
            print(f"Total size: {metrics.get('total_size_mb', 0):.1f} MB")
            if metrics.get('bounds'):
                b = metrics['bounds']
                print(f"Bounds (Web Mercator):")
                print(f"  X: {b['minx']:.0f} to {b['maxx']:.0f}")
                print(f"  Y: {b['miny']:.0f} to {b['maxy']:.0f}")

        elif stage == "aggregation":
            print(f"Tiles planned: {metrics.get('tile_count', 0)}")
            print(f"Tiles completed: {metrics.get('completed_count', 0)}")
            print(f"Max zoom: {metrics.get('max_zoom', 0)}")
            print(f"PMTiles created: {metrics.get('pmtiles_count', 0)}")
            print(f"PMTiles size: {metrics.get('pmtiles_size_mb', 0):.1f} MB")
            if metrics.get('sources_used'):
                print(f"Sources used: {', '.join(metrics['sources_used'])}")

        elif stage == "downsampling":
            print(f"Overview tiles: {metrics.get('tile_count', 0)}")
            print(f"Completed: {metrics.get('completed_count', 0)}")
            if metrics.get('zoom_levels'):
                print(f"Zoom levels: {', '.join(map(str, metrics['zoom_levels']))}")

        elif stage == "index":
            print(f"Index created: {'✓' if metrics.get('index_exists') else '✗'}")
            print(f"Coverage GeoJSON: {'✓' if metrics.get('coverage_exists') else '✗'}")
            print(f"Features: {metrics.get('feature_count', 0)}")

        print(f"{'='*60}\n")

    def generate_html_report(self, output_file: Path = None) -> Path:
        """Generate an HTML report with all metrics."""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.pipelines_dir / f"pipeline-report-{self.source}-{timestamp}.html"

        summary = self.state.get_summary(self.source)
        bounds = self.state.get_bounds(self.source)

        # Build HTML
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Pipeline Report: {self.source}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 40px auto;
            padding: 0 20px;
            background: #f5f5f5;
        }}
        h1 {{ color: #333; }}
        .summary {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            margin: 20px 0;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        th {{
            background: #4CAF50;
            color: white;
            font-weight: 600;
        }}
        .metric {{ color: #666; }}
        .value {{ font-weight: 600; color: #333; }}
        .completed {{ color: #4CAF50; }}
        .pending {{ color: #999; }}
        .viewer-link {{
            display: inline-block;
            background: #2196F3;
            color: white;
            padding: 10px 20px;
            border-radius: 4px;
            text-decoration: none;
            margin: 10px 0;
        }}
        .viewer-link:hover {{ background: #1976D2; }}
    </style>
</head>
<body>
    <h1>Pipeline Report: {self.source}</h1>
    <div class="summary">
        <p><strong>Generated:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        <p><strong>Completed Stages:</strong> {', '.join(summary['completed_stages']) if summary['completed_stages'] else 'None'}</p>
"""

        if bounds:
            center_lon = (bounds['min_lon'] + bounds['max_lon']) / 2
            center_lat = (bounds['min_lat'] + bounds['max_lat']) / 2
            html += f"""
        <p><strong>Coverage Bounds:</strong></p>
        <ul>
            <li>Longitude: {bounds['min_lon']:.4f} to {bounds['max_lon']:.4f}</li>
            <li>Latitude: {bounds['min_lat']:.4f} to {bounds['max_lat']:.4f}</li>
            <li>Center: {center_lat:.4f}, {center_lon:.4f}</li>
        </ul>
        <a href="index.html#{int(12)}/{center_lat:.6f}/{center_lon:.6f}" class="viewer-link">
            Open Viewer at Data Location
        </a>
"""

        html += """
    </div>
    <h2>Stage Metrics</h2>
    <table>
        <tr>
            <th>Stage</th>
            <th>Status</th>
            <th>Duration</th>
            <th>Key Metrics</th>
        </tr>
"""

        for stage in ["source_prep", "aggregation", "downsampling", "index"]:
            if stage in summary['stages']:
                stage_data = summary['stages'][stage]
                completion = stage_data['completion']
                metrics = stage_data['metrics']

                duration = completion.get('duration_seconds', 0)
                duration_str = f"{duration:.1f}s" if duration else "N/A"

                # Format key metrics
                key_metrics = []
                if stage == "source_prep" and 'file_count' in metrics:
                    key_metrics.append(f"{metrics['file_count']} files")
                    key_metrics.append(f"{metrics.get('total_size_mb', 0):.1f} MB")
                elif stage == "aggregation":
                    key_metrics.append(f"{metrics.get('tile_count', 0)} tiles")
                    key_metrics.append(f"{metrics.get('pmtiles_size_mb', 0):.1f} MB")
                elif stage == "downsampling":
                    key_metrics.append(f"{metrics.get('tile_count', 0)} overview tiles")
                elif stage == "index":
                    key_metrics.append(f"{metrics.get('feature_count', 0)} features")

                html += f"""
        <tr>
            <td>{stage.replace('_', ' ').title()}</td>
            <td class="completed">✓ Completed</td>
            <td>{duration_str}</td>
            <td>{', '.join(key_metrics)}</td>
        </tr>
"""
            else:
                html += f"""
        <tr>
            <td>{stage.replace('_', ' ').title()}</td>
            <td class="pending">Pending</td>
            <td>-</td>
            <td>-</td>
        </tr>
"""

        html += """
    </table>
</body>
</html>
"""

        output_file.write_text(html)
        return output_file
