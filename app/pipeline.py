"""Batch processing pipeline.

Every processing pass is a `run`: an immutable header plus one result row per
record, inserted in a single transaction. Replays create new runs instead of
touching old ones, so old data is never rewritten. Gray routing is a pure
function of (source, record_key), so replays reproduce the exact same split.
"""
from __future__ import annotations

import hashlib
from typing import Any

from .db import Database, dumps, loads, now_iso
from .plugins import parse_record


def record_key(batch_id: int, index: int) -> str:
    return f"b{batch_id}:{index}"


def route_to_gray(source_id: int, key: str, gray_percent: int) -> bool:
    """Deterministic per-record gray decision; stable across replays."""
    digest = hashlib.sha256(f"{source_id}:{key}".encode()).hexdigest()
    return int(digest, 16) % 100 < gray_percent


def _persist_run(db: Database, batch_id: int, source_id: int, version_id: int,
                 kind: str, outcomes: list[tuple[int, str, Any]]) -> int:
    """Insert one run + its results atomically. The only write path for
    processing output; callers do all parsing before entering here."""
    stats = {"total": len(outcomes), "ok": 0, "error": 0, "empty": 0}
    for _, _, oc in outcomes:
        stats[oc.status] += 1
    with db.transaction() as conn:
        run_id = conn.execute(
            "INSERT INTO runs(batch_id, source_id, version_id, kind, stats_json, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (batch_id, source_id, version_id, kind, dumps(stats), now_iso()),
        ).lastrowid
        conn.executemany(
            "INSERT INTO results(run_id, batch_id, record_index, record_key, status,"
            " canonical_json, extras_json, errors_json) VALUES (?,?,?,?,?,?,?,?)",
            [
                (run_id, batch_id, idx, key, oc.status,
                 dumps(oc.canonical), dumps(oc.extras), dumps(oc.errors))
                for idx, key, oc in outcomes
            ],
        )
    return run_id


def process_batch(db: Database, batch_id: int) -> list[int]:
    """Process a freshly uploaded batch with the source's current binding.

    Returns the created run ids. With gray enabled, records split into two
    runs (active + gray), each fully isolated.
    """
    batch = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if not batch:
        raise KeyError(f"batch {batch_id} not found")
    source = db.query_one("SELECT * FROM sources WHERE id=?", (batch["source_id"],))
    if not source or not source["active_version_id"]:
        raise ValueError("source has no active parser version")

    records = loads(batch["records_json"])
    specs = _spec_cache(db, [source["active_version_id"], source["gray_version_id"]])

    active_out, gray_out = [], []
    gray_pct = source["gray_percent"] if source["gray_version_id"] else 0
    for idx, raw in enumerate(records):
        key = record_key(batch_id, idx)
        if gray_pct and route_to_gray(source["id"], key, gray_pct):
            gray_out.append((idx, key, parse_record(specs[source["gray_version_id"]], raw)))
        else:
            active_out.append((idx, key, parse_record(specs[source["active_version_id"]], raw)))

    run_ids = []
    if active_out:
        run_ids.append(_persist_run(db, batch_id, source["id"],
                                    source["active_version_id"], "initial", active_out))
    if gray_out:
        run_ids.append(_persist_run(db, batch_id, source["id"],
                                    source["gray_version_id"], "gray", gray_out))
    return run_ids


def replay_batch(db: Database, batch_id: int, version_id: int) -> int:
    """Reprocess a batch with an explicit version. Creates a new run; the
    original runs and results are left untouched."""
    batch = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if not batch:
        raise KeyError(f"batch {batch_id} not found")
    spec = _spec_cache(db, [version_id])[version_id]
    records = loads(batch["records_json"])
    outcomes = [
        (idx, record_key(batch_id, idx), parse_record(spec, raw))
        for idx, raw in enumerate(records)
    ]
    return _persist_run(db, batch_id, batch["source_id"], version_id, "replay", outcomes)


def _spec_cache(db: Database, version_ids: list[int | None]) -> dict[int, dict]:
    out: dict[int, dict] = {}
    for vid in {v for v in version_ids if v}:
        row = db.query_one("SELECT spec_json FROM plugin_versions WHERE id=?", (vid,))
        if not row:
            raise KeyError(f"plugin version {vid} not found")
        out[vid] = loads(row["spec_json"])
    return out


# ------------------------------------------------------------------ metrics

def version_metrics(db: Database, source_id: int, version_id: int,
                    since: str | None = None) -> dict:
    """Aggregate ok/error/empty across a version's runs for one source."""
    sql = (
        "SELECT r.status AS status, COUNT(*) AS n FROM results r"
        " JOIN runs u ON u.id = r.run_id"
        " WHERE u.source_id=? AND u.version_id=? AND u.kind IN ('initial','gray')"
    )
    params: list[Any] = [source_id, version_id]
    if since:
        sql += " AND u.created_at >= ?"
        params.append(since)
    sql += " GROUP BY r.status"
    counts = {row["status"]: row["n"] for row in db.query(sql, tuple(params))}
    total = sum(counts.values())
    return {
        "total": total,
        "ok": counts.get("ok", 0),
        "error": counts.get("error", 0),
        "empty": counts.get("empty", 0),
        "error_rate": (counts.get("error", 0) / total) if total else 0.0,
        "empty_ratio": (counts.get("empty", 0) / total) if total else 0.0,
    }
