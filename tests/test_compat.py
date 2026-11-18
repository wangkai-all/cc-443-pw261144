"""Compatibility reports: diff detection, verdicts, publish gating."""
import pytest

from app import compat, monitor
from app.db import loads
from app.pipeline import process_batch
from tests.conftest import (bind_active, good_records, register, source_id,  # noqa: F401
                            spec_bad, spec_v1, spec_v2, upload, v2_records)


def _seed_history(db, source_id, v1, records=None):
    bind_active(db, source_id, v1)
    bid = upload(db, source_id, records or good_records(30))
    process_batch(db, bid)
    return bid


def test_report_fails_when_candidate_cannot_read_old_data(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    _seed_history(db, source_id, v1)
    v2 = register(db, "orders", "1.1.0", spec_v2(), status="draft")
    rid = compat.generate_report(db, source_id, v2)
    report = db.query_one("SELECT * FROM compat_reports WHERE id=?", (rid,))
    summary = loads(report["summary_json"])
    # strict v2 expects 5 columns; history has 4 -> column count mismatch
    assert report["verdict"] == "fail"
    assert summary["candidate_error_rate"] == 1.0


def test_report_passes_on_compatible_upgrade(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    _seed_history(db, source_id, v1, good_records(30))  # 4-col history
    v2 = register(db, "orders", "1.1.0", spec_v2(allow_missing=True), status="draft")
    rid = compat.generate_report(db, source_id, v2)
    report = db.query_one("SELECT * FROM compat_reports WHERE id=?", (rid,))
    summary = loads(report["summary_json"])
    assert report["verdict"] == "warn"          # new field appears, no failures
    assert summary["added_fields"] == ["loyalty_id"]
    assert summary["candidate_error_rate"] == 0
    samples = db.query("SELECT * FROM diff_samples WHERE report_id=?", (rid,))
    assert samples  # field diffs captured


def test_report_fail_blocks_publish_without_force(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    _seed_history(db, source_id, v1)
    bad = register(db, "orders", "2.0.0", spec_bad(), status="draft")
    rid = compat.generate_report(db, source_id, bad)
    assert db.query_one("SELECT verdict FROM compat_reports WHERE id=?",
                        (rid,))["verdict"] == "fail"
    with pytest.raises(monitor.PublishError):
        monitor.publish(db, source_id, bad)
    # force overrides
    monitor.publish(db, source_id, bad, force=True)
    assert db.query_one("SELECT active_version_id FROM sources WHERE id=?",
                        (source_id,))["active_version_id"] == bad


def test_publish_requires_report(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    v2 = register(db, "orders", "1.1.0", spec_v2(), status="draft")
    with pytest.raises(monitor.PublishError, match="report"):
        monitor.publish(db, source_id, v2)


def test_publish_full_replaces_active_and_records_binding_history(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    v2 = register(db, "orders", "1.1.0", spec_v2(), status="draft")
    compat.generate_report(db, source_id, v2)
    monitor.publish(db, source_id, v2)
    src = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    assert src["active_version_id"] == v2
    history = db.query("SELECT * FROM bindings WHERE source_id=? AND role='active'"
                       " ORDER BY id", (source_id,))
    assert len(history) == 2
    assert history[0]["version_id"] == v1 and history[0]["unbound_at"] is not None
    assert history[1]["version_id"] == v2 and history[1]["unbound_at"] is None


def test_publish_gray_keeps_active(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    v2 = register(db, "orders", "1.1.0", spec_v2(), status="draft")
    compat.generate_report(db, source_id, v2)
    monitor.publish(db, source_id, v2, gray_percent=25)
    src = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    assert src["active_version_id"] == v1
    assert src["gray_version_id"] == v2 and src["gray_percent"] == 25
