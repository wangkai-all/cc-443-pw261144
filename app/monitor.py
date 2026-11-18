"""Version lifecycle: publish, gray rollout, monitoring, rollback.

Auto-rollback evaluates the source's live metrics after every run. When a
version breaches the source's error-rate or empty-ratio thresholds (with a
big enough sample), the source is switched back to its previous binding.
Everything the bad version processed is preserved: its runs stay in place,
their ids are recorded on the rollback event, and fresh diff samples
(bad version vs. the version we roll back to) are captured for analysis.
"""
from __future__ import annotations

from .db import Database, dumps, loads, now_iso
from .pipeline import record_key, version_metrics
from .plugins import parse_record

MAX_DIFF_SAMPLES = 50


class PublishError(ValueError):
    pass


# ------------------------------------------------------------------ bindings

def _bind(db, source_id: int, version_id: int, role: str) -> None:
    db.execute(
        "INSERT INTO bindings(source_id, version_id, role, bound_at) VALUES (?,?,?,?)",
        (source_id, version_id, role, now_iso()))


def _unbind(db, source_id: int, role: str) -> None:
    db.execute(
        "UPDATE bindings SET unbound_at=? WHERE source_id=? AND role=? AND unbound_at IS NULL",
        (now_iso(), source_id, role))


def previous_active_version(db: Database, source_id: int) -> int | None:
    row = db.query_one(
        "SELECT b.version_id FROM bindings b"
        " JOIN plugin_versions v ON v.id = b.version_id"
        " WHERE b.source_id=? AND b.role='active' AND b.unbound_at IS NOT NULL"
        " AND v.status != 'rolled_back'"
        " ORDER BY b.unbound_at DESC LIMIT 1",
        (source_id,))
    return row["version_id"] if row else None


# ------------------------------------------------------------------ publish

def publish(db: Database, source_id: int, version_id: int, gray_percent: int = 0,
            force: bool = False) -> None:
    """Publish a candidate version onto a source.

    Requires a non-failing compatibility report unless force=True. With
    gray_percent > 0 the version becomes the gray (canary) binding; otherwise
    it becomes the active binding and the old active one is retired.
    """
    source = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    version = db.query_one("SELECT * FROM plugin_versions WHERE id=?", (version_id,))
    if not source or not version:
        raise KeyError("source or version not found")
    if version["status"] == "rolled_back":
        raise PublishError("cannot publish a rolled-back version")

    report = db.query_one(
        "SELECT * FROM compat_reports WHERE source_id=? AND candidate_version_id=?"
        " ORDER BY id DESC LIMIT 1", (source_id, version_id))
    if not report:
        raise PublishError("compatibility report required before publishing")
    if report["verdict"] == "fail" and not force:
        raise PublishError("compatibility report verdict is 'fail'; use force to override")

    now = now_iso()
    with db.transaction() as conn:
        conn.execute(
            "UPDATE plugin_versions SET status='published', published_at=? WHERE id=?",
            (now, version_id))
        conn.execute(
            "UPDATE bindings SET unbound_at=? WHERE source_id=? AND role='gray'"
            " AND unbound_at IS NULL", (now, source_id))
        if gray_percent > 0:
            conn.execute(
                "UPDATE sources SET gray_version_id=?, gray_percent=? WHERE id=?",
                (version_id, gray_percent, source_id))
            conn.execute(
                "INSERT INTO bindings(source_id, version_id, role, bound_at)"
                " VALUES (?,?,?,?)", (source_id, version_id, "gray", now))
        else:
            conn.execute(
                "UPDATE bindings SET unbound_at=? WHERE source_id=? AND role='active'"
                " AND unbound_at IS NULL", (now, source_id))
            conn.execute(
                "UPDATE sources SET active_version_id=?, gray_version_id=NULL,"
                " gray_percent=0 WHERE id=?", (version_id, source_id))
            conn.execute(
                "INSERT INTO bindings(source_id, version_id, role, bound_at)"
                " VALUES (?,?,?,?)", (source_id, version_id, "active", now))


def set_gray_percent(db: Database, source_id: int, percent: int) -> None:
    if not 0 <= percent <= 100:
        raise ValueError("gray percent must be 0..100")
    source = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    if not source:
        raise KeyError(f"source {source_id} not found")
    if percent and not source["gray_version_id"]:
        raise ValueError("no gray version bound to this source")
    db.execute("UPDATE sources SET gray_percent=? WHERE id=?", (percent, source_id))


# ------------------------------------------------------------------ rollback

def _collect_diff_samples(db: Database, source_id: int, bad_version_id: int,
                          good_version_id: int | None) -> list[dict]:
    """Compare what the bad version produced against what the rollback target
    produces on the same raw records. Pure re-parse; live data untouched."""
    if good_version_id is None:
        return []
    good = db.query_one("SELECT spec_json FROM plugin_versions WHERE id=?",
                        (good_version_id,))
    if not good:
        return []
    good_spec = loads(good["spec_json"])
    bad_runs = db.query(
        "SELECT id, batch_id FROM runs WHERE source_id=? AND version_id=?"
        " ORDER BY id DESC LIMIT 10", (source_id, bad_version_id))
    samples: list[dict] = []
    for run in bad_runs:
        batch = db.query_one("SELECT records_json FROM batches WHERE id=?",
                             (run["batch_id"],))
        if not batch:
            continue
        records = loads(batch["records_json"])
        bad_results = db.query(
            "SELECT record_index, status, canonical_json, errors_json FROM results"
            " WHERE run_id=?", (run["id"],))
        by_index = {r["record_index"]: r for r in bad_results}
        for idx, r in by_index.items():
            good_oc = parse_record(good_spec, records[idx])
            bad_canonical = loads(r["canonical_json"])
            if good_oc.canonical != bad_canonical or good_oc.status != r["status"]:
                samples.append({
                    "record_key": record_key(run["batch_id"], idx),
                    "baseline": {"status": r["status"], "canonical": bad_canonical,
                                 "errors": loads(r["errors_json"])},
                    "candidate": {"status": good_oc.status,
                                  "canonical": good_oc.canonical,
                                  "errors": good_oc.errors},
                })
                if len(samples) >= MAX_DIFF_SAMPLES:
                    return samples
    return samples


def rollback(db: Database, source_id: int, trigger: str, reason: str,
             metrics: dict | None = None, version_id: int | None = None) -> int | None:
    """Roll the source back from a bad version.

    `version_id` selects which binding to roll back; when omitted (manual
    rollback), the gray canary is removed first if one exists, otherwise the
    active version. Gray rollback just unbinds the canary; active rollback
    rebinds the previous active version. Returns the rollback event id, or
    None if there was nothing to roll back.
    """
    source = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    if not source:
        raise KeyError(f"source {source_id} not found")

    gray_id = source["gray_version_id"]
    active_id = source["active_version_id"]
    if version_id is None:
        version_id = gray_id or active_id
    if version_id is None:
        return None
    if version_id == gray_id:
        from_version, to_version, role = gray_id, active_id, "gray"
    elif version_id == active_id:
        from_version, role = active_id, "active"
        to_version = previous_active_version(db, source_id)
    else:
        raise ValueError(f"version {version_id} is not bound to source {source_id}")

    metrics = metrics or version_metrics(db, source_id, from_version)
    preserved = [r["id"] for r in db.query(
        "SELECT id FROM runs WHERE source_id=? AND version_id=? ORDER BY id",
        (source_id, from_version))]
    samples = _collect_diff_samples(db, source_id, from_version, to_version)

    now = now_iso()
    with db.transaction() as conn:
        event_id = conn.execute(
            "INSERT INTO rollback_events(source_id, from_version_id, to_version_id,"
            " trigger, reason, metrics_json, preserved_run_ids_json, created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (source_id, from_version, to_version, trigger, reason,
             dumps(metrics), dumps(preserved), now),
        ).lastrowid
        conn.executemany(
            "INSERT INTO diff_samples(rollback_event_id, record_key, baseline_json,"
            " candidate_json) VALUES (?,?,?,?)",
            [(event_id, s["record_key"], dumps(s["baseline"]), dumps(s["candidate"]))
             for s in samples],
        )
        conn.execute(
            "UPDATE plugin_versions SET status='rolled_back' WHERE id=?", (from_version,))
        if role == "gray":
            conn.execute(
                "UPDATE bindings SET unbound_at=? WHERE source_id=? AND role='gray'"
                " AND unbound_at IS NULL", (now, source_id))
            conn.execute(
                "UPDATE sources SET gray_version_id=NULL, gray_percent=0 WHERE id=?",
                (source_id,))
        else:
            conn.execute(
                "UPDATE bindings SET unbound_at=? WHERE source_id=? AND role='active'"
                " AND unbound_at IS NULL", (now, source_id))
            if to_version is not None:
                conn.execute(
                    "UPDATE sources SET active_version_id=? WHERE id=?",
                    (to_version, source_id))
                conn.execute(
                    "INSERT INTO bindings(source_id, version_id, role, bound_at)"
                    " VALUES (?,?,?,?)", (source_id, to_version, "active", now))
            else:
                conn.execute(
                    "UPDATE sources SET active_version_id=NULL WHERE id=?", (source_id,))
    return event_id


# ------------------------------------------------------------------ monitor

def evaluate_source(db: Database, source_id: int) -> int | None:
    """Check live metrics after a run; auto-rollback on threshold breach.

    Returns the rollback event id if a rollback happened. Gray bindings are
    checked first, then the active binding. A minimum sample size keeps tiny
    batches from tripping the alarm.
    """
    source = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    if not source:
        return None
    for version_id, label in ((source["gray_version_id"], "gray"),
                              (source["active_version_id"], "active")):
        if not version_id:
            continue
        metrics = version_metrics(db, source_id, version_id)
        if metrics["total"] < source["min_sample"]:
            continue
        reasons = []
        if metrics["error_rate"] > source["error_threshold"]:
            reasons.append(
                f"error rate {metrics['error_rate']:.2%} > "
                f"{source['error_threshold']:.2%}")
        if metrics["empty_ratio"] > source["empty_threshold"]:
            reasons.append(
                f"empty ratio {metrics['empty_ratio']:.2%} > "
                f"{source['empty_threshold']:.2%}")
        if reasons:
            return rollback(db, source_id, trigger="auto",
                            reason=f"{label} version breached: " + "; ".join(reasons),
                            metrics=metrics, version_id=version_id)
    return None
