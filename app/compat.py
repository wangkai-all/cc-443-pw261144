"""Pre-publish compatibility reports.

Runs the candidate version and the current baseline side by side, in memory,
over a sample of the source's recent raw records. Nothing is written to
runs/results -- the report and its diff samples are the only artifacts, so a
dry run can never pollute live processing output.
"""
from __future__ import annotations

from .db import Database, dumps, loads, now_iso
from .pipeline import record_key
from .plugins import parse_record

MAX_DIFF_SAMPLES = 50


def _recent_raw_records(db: Database, source_id: int, limit: int) -> list[tuple[str, object]]:
    """Newest raw records across the source's batches, oldest-first."""
    batches = db.query(
        "SELECT id, records_json FROM batches WHERE source_id=? ORDER BY id DESC LIMIT 20",
        (source_id,))
    sample: list[tuple[str, object]] = []
    for batch in batches:  # newest batch first; reverse at the end
        for idx, raw in enumerate(loads(batch["records_json"])):
            sample.append((record_key(batch["id"], idx), raw))
    sample.reverse()
    return sample[-limit:]


def generate_report(db: Database, source_id: int, candidate_version_id: int,
                    sample_size: int = 200) -> int:
    source = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    if not source:
        raise KeyError(f"source {source_id} not found")
    candidate = db.query_one("SELECT * FROM plugin_versions WHERE id=?",
                             (candidate_version_id,))
    if not candidate:
        raise KeyError(f"plugin version {candidate_version_id} not found")
    cand_spec = loads(candidate["spec_json"])

    baseline_id = source["active_version_id"]
    base_spec = None
    if baseline_id:
        row = db.query_one("SELECT spec_json FROM plugin_versions WHERE id=?",
                           (baseline_id,))
        base_spec = loads(row["spec_json"]) if row else None

    records = _recent_raw_records(db, source_id, sample_size)
    summary = {
        "records": len(records),
        "baseline_version_id": baseline_id,
        "candidate_version_id": candidate_version_id,
        "candidate_errors": 0,
        "candidate_empty": 0,
        "status_flips": {},          # e.g. "ok->error": 3
        "field_diffs": 0,            # records whose canonical output differs
        "added_fields": [],          # canonical fields only the candidate emits
        "removed_fields": [],        # canonical fields only the baseline emits
        "notes": [],
    }
    diff_samples: list[dict] = []
    added: set[str] = set()
    removed: set[str] = set()

    for key, raw in records:
        cand = parse_record(cand_spec, raw)
        if cand.status == "error":
            summary["candidate_errors"] += 1
        elif cand.status == "empty":
            summary["candidate_empty"] += 1
        if base_spec is None:
            continue
        base = parse_record(base_spec, raw)
        if base.status != cand.status:
            flip = f"{base.status}->{cand.status}"
            summary["status_flips"][flip] = summary["status_flips"].get(flip, 0) + 1
        added |= set(cand.canonical) - set(base.canonical)
        removed |= set(base.canonical) - set(cand.canonical)
        if base.canonical != cand.canonical or base.status != cand.status:
            summary["field_diffs"] += 1
            if len(diff_samples) < MAX_DIFF_SAMPLES:
                diff_samples.append({
                    "record_key": key,
                    "baseline": {"status": base.status, "canonical": base.canonical,
                                 "errors": base.errors},
                    "candidate": {"status": cand.status, "canonical": cand.canonical,
                                  "errors": cand.errors},
                })

    summary["added_fields"] = sorted(added)
    summary["removed_fields"] = sorted(removed)
    total = max(summary["records"], 1)
    error_rate = summary["candidate_errors"] / total
    empty_ratio = summary["candidate_empty"] / total
    summary["candidate_error_rate"] = round(error_rate, 4)
    summary["candidate_empty_ratio"] = round(empty_ratio, 4)

    if not records:
        verdict = "warn"
        summary["notes"].append("no historical records to sample")
    elif error_rate > source["error_threshold"] or empty_ratio > source["empty_threshold"]:
        verdict = "fail"
        summary["notes"].append(
            f"candidate would breach thresholds "
            f"(error {error_rate:.2%} > {source['error_threshold']:.2%} or "
            f"empty {empty_ratio:.2%} > {source['empty_threshold']:.2%})")
    elif summary["status_flips"] or summary["field_diffs"] or added or removed:
        verdict = "warn"
    else:
        verdict = "pass"
    if base_spec is None:
        summary["notes"].append("no baseline version bound; nothing to compare against")

    with db.transaction() as conn:
        report_id = conn.execute(
            "INSERT INTO compat_reports(source_id, baseline_version_id,"
            " candidate_version_id, sample_size, summary_json, verdict, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (source_id, baseline_id, candidate_version_id, len(records),
             dumps(summary), verdict, now_iso()),
        ).lastrowid
        conn.executemany(
            "INSERT INTO diff_samples(report_id, record_key, baseline_json, candidate_json)"
            " VALUES (?,?,?,?)",
            [(report_id, s["record_key"], dumps(s["baseline"]), dumps(s["candidate"]))
             for s in diff_samples],
        )
    return report_id
