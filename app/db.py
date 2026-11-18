"""SQLite storage layer.

All mutable state lives here. Processing runs and their results are immutable
once written: replays and rollbacks never update or delete past runs, they only
create new rows. That property is what keeps old data, new fields and parallel
batches from polluting each other.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  active_version_id INTEGER,
  gray_version_id INTEGER,
  gray_percent INTEGER NOT NULL DEFAULT 0,
  error_threshold REAL NOT NULL DEFAULT 0.05,
  empty_threshold REAL NOT NULL DEFAULT 0.20,
  min_sample INTEGER NOT NULL DEFAULT 20,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plugin_versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft',   -- draft | published | rolled_back
  created_at TEXT NOT NULL,
  published_at TEXT,
  UNIQUE(name, version)
);
CREATE TABLE IF NOT EXISTS bindings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  version_id INTEGER NOT NULL,
  role TEXT NOT NULL,                     -- active | gray
  bound_at TEXT NOT NULL,
  unbound_at TEXT
);
CREATE TABLE IF NOT EXISTS batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  records_json TEXT NOT NULL,             -- raw payloads, never rewritten
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  version_id INTEGER NOT NULL,
  kind TEXT NOT NULL,                     -- initial | gray | replay
  stats_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  batch_id INTEGER NOT NULL,
  record_index INTEGER NOT NULL,
  record_key TEXT NOT NULL,
  status TEXT NOT NULL,                   -- ok | error | empty
  canonical_json TEXT NOT NULL,
  extras_json TEXT NOT NULL,
  errors_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_results_run ON results(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_source_version ON runs(source_id, version_id);
CREATE TABLE IF NOT EXISTS compat_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  baseline_version_id INTEGER,
  candidate_version_id INTEGER NOT NULL,
  sample_size INTEGER NOT NULL,
  summary_json TEXT NOT NULL,
  verdict TEXT NOT NULL,                  -- pass | warn | fail
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS diff_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  report_id INTEGER,
  rollback_event_id INTEGER,
  record_key TEXT NOT NULL,
  baseline_json TEXT,
  candidate_json TEXT
);
CREATE TABLE IF NOT EXISTS rollback_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  from_version_id INTEGER NOT NULL,
  to_version_id INTEGER,
  trigger TEXT NOT NULL,                  -- auto | manual
  reason TEXT NOT NULL,
  metrics_json TEXT NOT NULL,
  preserved_run_ids_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Database:
    """Thread-safe SQLite wrapper.

    A single write lock serializes transactions; parsing happens outside the
    lock so concurrent batch processing cannot interleave writes. Each run's
    results are inserted in one transaction keyed by run_id, which is what
    isolates parallel batches from each other.
    """

    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    @contextmanager
    def transaction(self):
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def query_one(self, sql: str, params: tuple = ()) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: tuple = ()) -> int:
        """Single statement inside the write lock. Returns lastrowid."""
        with self.transaction() as conn:
            cur = conn.execute(sql, params)
            return cur.lastrowid

    def close(self):
        with self._lock:
            self._conn.close()


def dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def loads(text: str):
    return json.loads(text)
