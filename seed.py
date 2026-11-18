"""Seed a demo scenario: one source, three plugin versions, batches,
a gray rollout and one auto-rollback, so the UI has data on first boot.

Usage:  python3 seed.py [db_path]     (default: parser_plugins.db)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app import compat, monitor, pipeline
from app.db import Database, dumps, now_iso
from tests.conftest import good_records, spec_bad, spec_v1, spec_v2


def main(db_path: str = "parser_plugins.db") -> None:
    db = Database(db_path)
    if db.query_one("SELECT id FROM sources LIMIT 1"):
        print("database already seeded; skipping")
        return

    src = db.execute(
        "INSERT INTO sources(name, error_threshold, empty_threshold, min_sample,"
        " created_at) VALUES ('orders_feed', 0.05, 0.20, 20, ?)", (now_iso(),))

    def reg(name, ver, spec):
        return db.execute(
            "INSERT INTO plugin_versions(name, version, spec_json, status, created_at)"
            " VALUES (?,?,?,?,?)", (name, ver, dumps(spec), "draft", now_iso()))

    v1 = reg("orders_csv", "1.0.0", spec_v1())
    v2 = reg("orders_csv", "1.1.0", spec_v2(allow_missing=True))
    v3 = reg("orders_csv", "2.0.0", spec_bad())

    # v1 live, some history
    compat.generate_report(db, src, v1)
    monitor.publish(db, src, v1)
    for i in range(3):
        bid = db.execute(
            "INSERT INTO batches(source_id, name, records_json, created_at)"
            " VALUES (?,?,?,?)",
            (src, f"orders-{i:03d}", dumps(good_records(30, i * 1000)), now_iso()))
        pipeline.process_batch(db, bid)

    # v2 canary at 30% (reads the old 4-column shape fine)
    compat.generate_report(db, src, v2)
    monitor.publish(db, src, v2, gray_percent=30)
    bid = db.execute(
        "INSERT INTO batches(source_id, name, records_json, created_at)"
        " VALUES (?,?,?,?)",
        (src, "orders-gray-1", dumps(good_records(40, 9000)), now_iso()))
    pipeline.process_batch(db, bid)
    monitor.evaluate_source(db, src)

    # v3 full release -> broken -> auto rollback after enough bad records
    compat.generate_report(db, src, v3)
    try:
        monitor.publish(db, src, v3)  # refused: report verdict is fail
    except monitor.PublishError:
        monitor.publish(db, src, v3, force=True)
    bid = db.execute(
        "INSERT INTO batches(source_id, name, records_json, created_at)"
        " VALUES (?,?,?,?)",
        (src, "orders-broken-1", dumps(good_records(30, 5000)), now_iso()))
    pipeline.process_batch(db, bid)
    event_id = monitor.evaluate_source(db, src)
    print(f"seeded {db_path}; auto-rollback event: #{event_id}")
    db.close()


if __name__ == "__main__":
    main(*sys.argv[1:])
