"""Pipeline: batch processing, deterministic gray split, replay semantics."""
from app.db import loads
from app.pipeline import process_batch, replay_batch, route_to_gray
from tests.conftest import (bind_active, good_records, register, source_id,  # noqa: F401
                            spec_v1, spec_v2, upload)


def test_process_batch_creates_run_and_results(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)
    bid = upload(db, source_id, good_records(10))
    run_ids = process_batch(db, bid)
    assert len(run_ids) == 1
    run = db.query_one("SELECT * FROM runs WHERE id=?", (run_ids[0],))
    assert run["version_id"] == v1 and run["kind"] == "initial"
    stats = loads(run["stats_json"])
    assert stats == {"total": 10, "ok": 10, "error": 0, "empty": 0}
    results = db.query("SELECT * FROM results WHERE run_id=?", (run_ids[0],))
    assert len(results) == 10
    assert {r["record_key"] for r in results} == {f"b{bid}:{i}" for i in range(10)}


def test_gray_split_is_deterministic_and_partitioned(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    v2 = register(db, "orders", "1.1.0", spec_v2())
    bind_active(db, source_id, v1)
    db.execute("UPDATE sources SET gray_version_id=?, gray_percent=30 WHERE id=?",
               (v2, source_id))
    records = good_records(200)
    bid = upload(db, source_id, records)
    run_ids = process_batch(db, bid)
    assert len(run_ids) == 2

    runs = {r["kind"]: r for r in db.query("SELECT * FROM runs WHERE batch_id=?", (bid,))}
    gray_keys = {r["record_key"] for r in
                 db.query("SELECT record_key FROM results WHERE run_id=?",
                          (runs["gray"]["id"],))}
    main_keys = {r["record_key"] for r in
                 db.query("SELECT record_key FROM results WHERE run_id=?",
                          (runs["initial"]["id"],))}
    # partition: no overlap, full coverage
    assert gray_keys.isdisjoint(main_keys)
    assert len(gray_keys | main_keys) == 200
    # deterministic: routing function alone reproduces the same split
    for i in range(200):
        key = f"b{bid}:{i}"
        expect_gray = route_to_gray(source_id, key, 30)
        assert (key in gray_keys) == expect_gray
    # sanity: split is not degenerate
    assert 0 < len(gray_keys) < 200


def test_replay_creates_new_run_and_preserves_original(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    v2 = register(db, "orders", "1.1.0", spec_v2())
    bind_active(db, source_id, v1)
    bid = upload(db, source_id, good_records(5))
    (run1,) = process_batch(db, bid)
    before = db.query("SELECT * FROM results WHERE run_id=? ORDER BY record_index",
                      (run1,))

    run2 = replay_batch(db, bid, v2)
    assert run2 != run1
    after = db.query("SELECT * FROM results WHERE run_id=? ORDER BY record_index",
                     (run1,))
    assert before == after  # original run untouched
    replay_run = db.query_one("SELECT * FROM runs WHERE id=?", (run2,))
    assert replay_run["kind"] == "replay"
    assert replay_run["version_id"] == v2


def test_processing_without_active_version_raises(db, source_id):
    bid = upload(db, source_id, good_records(3))
    import pytest
    with pytest.raises(ValueError):
        process_batch(db, bid)
