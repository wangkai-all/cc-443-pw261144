"""Auto/manual rollback: thresholds, preservation, gray handling."""
from app import monitor
from app.db import loads
from app.pipeline import process_batch
from tests.conftest import (bind_active, good_records, register, source_id,  # noqa: F401
                            spec_bad, spec_empty_prone, spec_v1, upload)


def _upgrade(db, source_id, v1, new_spec, name="2.0.0"):
    """Publish a new active version the way the UI would (report + publish)."""
    from app import compat
    v2 = register(db, "orders", name, new_spec, status="draft")
    compat.generate_report(db, source_id, v2)
    monitor.publish(db, source_id, v2, force=True)
    return v2


def test_auto_rollback_on_error_rate(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    process_batch(db, upload(db, source_id, good_records(30)))
    bad = _upgrade(db, source_id, v1, spec_bad())

    # bad version processes a batch -> 100% errors -> evaluate triggers rollback
    bid = upload(db, source_id, good_records(30, start=100))
    process_batch(db, bid)
    event_id = monitor.evaluate_source(db, source_id)
    assert event_id is not None

    src = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    assert src["active_version_id"] == v1            # switched back
    assert db.query_one("SELECT status FROM plugin_versions WHERE id=?",
                        (bad,))["status"] == "rolled_back"
    event = db.query_one("SELECT * FROM rollback_events WHERE id=?", (event_id,))
    assert event["trigger"] == "auto"
    assert "error rate" in event["reason"]
    metrics = loads(event["metrics_json"])
    assert metrics["error_rate"] == 1.0

    # processed batches preserved: the bad run still exists with its results
    preserved = loads(event["preserved_run_ids_json"])
    assert preserved
    for run_id in preserved:
        assert db.query("SELECT id FROM results WHERE run_id=?", (run_id,))
    # diff samples captured (bad vs rollback target)
    samples = db.query("SELECT * FROM diff_samples WHERE rollback_event_id=?",
                       (event_id,))
    assert samples


def test_auto_rollback_on_empty_ratio(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    ghost = _upgrade(db, source_id, v1, spec_empty_prone())
    bid = upload(db, source_id, good_records(25))
    process_batch(db, bid)  # all records parse to all-blank canonical -> empty
    event_id = monitor.evaluate_source(db, source_id)
    assert event_id is not None
    event = db.query_one("SELECT * FROM rollback_events WHERE id=?", (event_id,))
    assert "empty ratio" in event["reason"]
    assert db.query_one("SELECT active_version_id FROM sources WHERE id=?",
                        (source_id,))["active_version_id"] == v1


def test_min_sample_prevents_premature_rollback(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    _upgrade(db, source_id, v1, spec_bad())
    bid = upload(db, source_id, good_records(5))  # below min_sample=20
    process_batch(db, bid)
    assert monitor.evaluate_source(db, source_id) is None
    src = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    assert src["active_version_id"] != v1  # still on the new version


def test_gray_breach_rolls_back_gray_only(db, source_id):
    from app import compat
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    bad = register(db, "orders", "2.0.0", spec_bad(), status="draft")
    compat.generate_report(db, source_id, bad)
    monitor.publish(db, source_id, bad, gray_percent=100, force=True)

    process_batch(db, upload(db, source_id, good_records(30)))
    event_id = monitor.evaluate_source(db, source_id)
    assert event_id is not None
    src = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    assert src["gray_version_id"] is None and src["gray_percent"] == 0
    assert src["active_version_id"] == v1  # active untouched
    assert db.query_one("SELECT status FROM plugin_versions WHERE id=?",
                        (bad,))["status"] == "rolled_back"


def test_healthy_version_does_not_rollback(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    process_batch(db, upload(db, source_id, good_records(50)))
    assert monitor.evaluate_source(db, source_id) is None


def test_rolled_back_version_cannot_be_republished(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    bad = _upgrade(db, source_id, v1, spec_bad())
    process_batch(db, upload(db, source_id, good_records(30)))
    monitor.evaluate_source(db, source_id)
    import pytest
    with pytest.raises(monitor.PublishError, match="rolled-back"):
        monitor.publish(db, source_id, bad, force=True)


def test_active_breach_with_gray_bound_rolls_back_active_not_gray(db, source_id):
    """Regression: rollback must target the breaching version. A gray canary
    must not be sacrificed when the active version is the one misbehaving."""
    from app import compat
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    bad_active = _upgrade(db, source_id, v1, spec_bad())   # full release first
    healthy_gray = register(db, "orders", "1.1.0", spec_v1(), status="draft")
    compat.generate_report(db, source_id, healthy_gray)    # then start a canary
    monitor.publish(db, source_id, healthy_gray, gray_percent=10)

    process_batch(db, upload(db, source_id, good_records(30)))
    event_id = monitor.evaluate_source(db, source_id)
    assert event_id is not None
    event = db.query_one("SELECT * FROM rollback_events WHERE id=?", (event_id,))
    assert event["from_version_id"] == bad_active
    src = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    assert src["active_version_id"] == v1                 # active rolled back
    assert src["gray_version_id"] == healthy_gray         # gray canary survives
    assert db.query_one("SELECT status FROM plugin_versions WHERE id=?",
                        (healthy_gray,))["status"] == "published"


def test_rollback_never_returns_to_a_rolled_back_version(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    bad = _upgrade(db, source_id, v1, spec_bad())
    process_batch(db, upload(db, source_id, good_records(30)))
    monitor.evaluate_source(db, source_id)          # bad -> v1, bad rolled_back
    # v1 breaks later (data drift) and gets rolled back itself
    db.execute("UPDATE sources SET error_threshold=0.0 WHERE id=?", (source_id,))
    process_batch(db, upload(db, source_id, good_records(30)))
    # force a breach on v1 by flipping its results to errors
    db.execute("UPDATE results SET status='error' WHERE run_id IN"
               " (SELECT id FROM runs WHERE version_id=?)", (v1,))
    event_id = monitor.evaluate_source(db, source_id)
    assert event_id is not None
    event = db.query_one("SELECT * FROM rollback_events WHERE id=?", (event_id,))
    assert event["to_version_id"] is None or event["to_version_id"] != bad
    src = db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
    assert src["active_version_id"] != bad


def test_manual_rollback(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    v2 = _upgrade(db, source_id, v1, spec_v1(), name="1.0.1")
    event_id = monitor.rollback(db, source_id, trigger="manual", reason="operator")
    assert event_id is not None
    event = db.query_one("SELECT * FROM rollback_events WHERE id=?", (event_id,))
    assert event["trigger"] == "manual"
    assert event["from_version_id"] == v2 and event["to_version_id"] == v1
    assert db.query_one("SELECT active_version_id FROM sources WHERE id=?",
                        (source_id,))["active_version_id"] == v1
