"""Pipeline state management for tracking completion and metrics across pipeline stages."""

import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import contextmanager


class PipelineState:
    """Manages pipeline execution state and metrics in SQLite database."""

    def __init__(self, db_path: Path = Path(".pipeline_state.db")):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize database schema if needed."""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stage_completion (
                    source TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    duration_seconds REAL,
                    PRIMARY KEY (source, stage)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stage_metrics (
                    source TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value TEXT NOT NULL,
                    PRIMARY KEY (source, stage, metric_name)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stage_bounds (
                    source TEXT NOT NULL,
                    min_lon REAL,
                    min_lat REAL,
                    max_lon REAL,
                    max_lat REAL,
                    PRIMARY KEY (source)
                )
            """)
            conn.commit()

    @contextmanager
    def _get_conn(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def mark_stage_complete(self, source: str, stage: str, duration_seconds: Optional[float] = None):
        """Mark a pipeline stage as completed."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO stage_completion (source, stage, completed_at, duration_seconds)
                VALUES (?, ?, ?, ?)
            """, (source, stage, datetime.utcnow().isoformat(), duration_seconds))
            conn.commit()

    def is_stage_complete(self, source: str, stage: str) -> bool:
        """Check if a stage has been completed."""
        with self._get_conn() as conn:
            result = conn.execute("""
                SELECT 1 FROM stage_completion
                WHERE source = ? AND stage = ?
            """, (source, stage)).fetchone()
            return result is not None

    def get_stage_completion(self, source: str, stage: str) -> Optional[Dict[str, Any]]:
        """Get completion info for a stage."""
        with self._get_conn() as conn:
            result = conn.execute("""
                SELECT completed_at, duration_seconds
                FROM stage_completion
                WHERE source = ? AND stage = ?
            """, (source, stage)).fetchone()
            if result:
                return {
                    "completed_at": result["completed_at"],
                    "duration_seconds": result["duration_seconds"]
                }
            return None

    def set_metric(self, source: str, stage: str, metric_name: str, value: Any):
        """Store a metric value (will be JSON serialized)."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO stage_metrics (source, stage, metric_name, metric_value)
                VALUES (?, ?, ?, ?)
            """, (source, stage, metric_name, json.dumps(value)))
            conn.commit()

    def get_metric(self, source: str, stage: str, metric_name: str) -> Optional[Any]:
        """Retrieve a metric value."""
        with self._get_conn() as conn:
            result = conn.execute("""
                SELECT metric_value FROM stage_metrics
                WHERE source = ? AND stage = ? AND metric_name = ?
            """, (source, stage, metric_name)).fetchone()
            if result:
                return json.loads(result["metric_value"])
            return None

    def get_all_metrics(self, source: str, stage: str) -> Dict[str, Any]:
        """Get all metrics for a stage."""
        with self._get_conn() as conn:
            results = conn.execute("""
                SELECT metric_name, metric_value FROM stage_metrics
                WHERE source = ? AND stage = ?
            """, (source, stage)).fetchall()
            return {row["metric_name"]: json.loads(row["metric_value"]) for row in results}

    def set_bounds(self, source: str, min_lon: float, min_lat: float, max_lon: float, max_lat: float):
        """Store geographic bounds for processed data."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO stage_bounds (source, min_lon, min_lat, max_lon, max_lat)
                VALUES (?, ?, ?, ?, ?)
            """, (source, min_lon, min_lat, max_lon, max_lat))
            conn.commit()

    def get_bounds(self, source: str) -> Optional[Dict[str, float]]:
        """Get geographic bounds for a source."""
        with self._get_conn() as conn:
            result = conn.execute("""
                SELECT min_lon, min_lat, max_lon, max_lat
                FROM stage_bounds
                WHERE source = ?
            """, (source,)).fetchone()
            if result:
                return {
                    "min_lon": result["min_lon"],
                    "min_lat": result["min_lat"],
                    "max_lon": result["max_lon"],
                    "max_lat": result["max_lat"]
                }
            return None

    def get_completed_stages(self, source: str) -> List[str]:
        """Get list of completed stages for a source."""
        with self._get_conn() as conn:
            results = conn.execute("""
                SELECT stage FROM stage_completion
                WHERE source = ?
                ORDER BY completed_at
            """, (source,)).fetchall()
            return [row["stage"] for row in results]

    def clear_source(self, source: str):
        """Remove all state for a source."""
        with self._get_conn() as conn:
            conn.execute("DELETE FROM stage_completion WHERE source = ?", (source,))
            conn.execute("DELETE FROM stage_metrics WHERE source = ?", (source,))
            conn.execute("DELETE FROM stage_bounds WHERE source = ?", (source,))
            conn.commit()

    def get_summary(self, source: str) -> Dict[str, Any]:
        """Get complete summary of source processing state."""
        completed = self.get_completed_stages(source)
        summary = {
            "source": source,
            "completed_stages": completed,
            "bounds": self.get_bounds(source),
            "stages": {}
        }

        for stage in completed:
            completion = self.get_stage_completion(source, stage)
            metrics = self.get_all_metrics(source, stage)
            summary["stages"][stage] = {
                "completion": completion,
                "metrics": metrics
            }

        return summary
