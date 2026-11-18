"""Isolation guarantees: new fields, old data, parallel batches."""
from concurrent.futures import ThreadPoolExecutor

from app import monitor
from app.db import loads
from app.pipeline import process_batch, replay_batch
from tests.conftest import (bind_active, good_records, register, source_id,  # noqa: F401
                            spec_v1, spec_v2, upload, v2_records)
from app import compat


V1_FIELDS = {"order_id", "amount", "currency", "email"}


def test_new_fields_never_leak_into_old_version_results(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    v2 = register(db, "orders", "1.1.0", spec_v2())
    bind_active(db, source_id, v1)

    # batch 1 processed by v1
    b1 = upload(db, source_id, good_records(10))
    (run1,) = process_batch(db, b1)
    # upgrade; batch 2 (5-column shape) processed by v2
    compat.generate_report(db, source_id, v2)
    monitor.publish(db, source_id, v2, force=True)
    b2 = upload(db, source_id, v2_records(10))
    (run2,) = process_batch(db, b2)

    # v1 run: exactly the v1 field set, no loyalty_id anywhere
    for r in db.query("SELECT canonical_json, extras_json FROM results WHERE run_id=?",
                      (run1,)):
        canonical = loads(r["canonical_json"])
        assert set(canonical) == V1_FIELDS
        assert "loyalty_id" not in canonical
        assert "loyalty_id" not in loads(r["extras_json"])
    # v2 run: loyalty_id present in canonical (it is a mapped field there)
    for r in db.query("SELECT canonical_json FROM results WHERE run_id=?", (run2,)):
        assert set(loads(r["canonical_json"])) == V1_FIELDS | {"loyalty_id"}

    # replay the NEW 5-column batch with the OLD v1 spec: v1 rejects the
    # shape (column count mismatch) instead of absorbing unknown fields
    run3 = replay_batch(db, b2, v1)
    for r in db.query("SELECT status, canonical_json, extras_json FROM results"
                      " WHERE run_id=?", (run3,)):
        assert r["status"] == "error"
        assert "loyalty_id" not in loads(r["canonical_json"])
        assert "loyalty_id" not in loads(r["extras_json"])


def test_old_data_survives_upgrade_and_replay(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    v2 = register(db, "orders", "1.1.0", spec_v2())
    bind_active(db, source_id, v1)
    bid = upload(db, source_id, good_records(10))
    (run1,) = process_batch(db, bid)

    raw_before = db.query_one("SELECT records_json FROM batches WHERE id=?",
                              (bid,))["records_json"]
    results_before = db.query(
        "SELECT * FROM results WHERE run_id=? ORDER BY record_index", (run1,))

    compat.generate_report(db, source_id, v2)
    monitor.publish(db, source_id, v2, force=True)
    process_batch(db, upload(db, source_id, v2_records(10)))
    replay_batch(db, bid, v2)

    # raw payloads immutable, original run byte-identical
    assert db.query_one("SELECT records_json FROM batches WHERE id=?",
                        (bid,))["records_json"] == raw_before
    assert db.query("SELECT * FROM results WHERE run_id=? ORDER BY record_index",
                    (run1,)) == results_before
    # replay produced a separate run, not a mutation
    runs = db.query("SELECT id FROM runs WHERE batch_id=?", (bid,))
    assert len(runs) == 2


def test_parallel_batches_do_not_pollute_each_other(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    bind_active(db, source_id, v1)

    n_batches, per_batch = 8, 25
    batch_ids = [
        upload(db, source_id, good_records(per_batch, start=b * 1000),
               name=f"parallel-{b}")
        for b in range(n_batches)
    ]

    with ThreadPoolExecutor(max_workers=n_batches) as pool:
        run_id_lists = list(pool.map(lambda b: process_batch(db, b), batch_ids))

    assert len(run_id_lists) == n_batches
    all_keys = set()
    for bid, run_ids in zip(batch_ids, run_id_lists):
        assert len(run_ids) == 1
        results = db.query("SELECT record_key, canonical_json FROM results"
                           " WHERE run_id=?", (run_ids[0],))
        # each run contains exactly its own batch's records, nothing else
        assert len(results) == per_batch
        keys = {r["record_key"] for r in results}
        assert keys == {f"b{bid}:{i}" for i in range(per_batch)}
        assert keys.isdisjoint(all_keys)
        all_keys |= keys
        # spot-check content matches the batch's own payloads
        first = loads(results[0]["canonical_json"])
        assert first["order_id"].startswith("ORD-")
    assert len(all_keys) == n_batches * per_batch
    # no stray runs/results from other batches
    assert db.query_one("SELECT COUNT(*) AS n FROM runs")["n"] == n_batches
    assert db.query_one("SELECT COUNT(*) AS n FROM results")["n"] == \
        n_batches * per_batch


def test_parallel_replays_stay_separate(db, source_id):
    v1 = register(db, "orders", "1.0.0", spec_v1())
    v2 = register(db, "orders", "1.1.0", spec_v2())
    bind_active(db, source_id, v1)
    bids = [upload(db, source_id, good_records(15, start=b * 100)) for b in range(4)]
    for b in bids:
        process_batch(db, b)

    with ThreadPoolExecutor(max_workers=4) as pool:
        replay_runs = list(pool.map(lambda b: replay_batch(db, b, v2), bids))
    assert len(set(replay_runs)) == 4
    for bid, rid in zip(bids, replay_runs):
        keys = {r["record_key"] for r in
                db.query("SELECT record_key FROM results WHERE run_id=?", (rid,))}
        assert keys == {f"b{bid}:{i}" for i in range(15)}
