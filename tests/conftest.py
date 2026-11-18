import pytest

from app.db import Database
from app.main import create_app


def spec_v1():
    """orders_csv v1: four-column pipe-delimited feed."""
    return {
        "parser": {"format": "delimited", "delimiter": "|", "trim": True,
                   "columns": ["order_id", "amount_raw", "currency", "email"]},
        "field_mapping": {
            "order_id": {"target": "order_id", "type": "string", "required": True},
            "amount_raw": {"target": "amount", "type": "decimal", "required": True},
            "currency": {"target": "currency", "type": "string", "default": "USD"},
            "email": {"target": "email", "type": "string"},
        },
        "validation": [
            {"field": "amount", "rule": "range", "min": 0},
            {"field": "order_id", "rule": "regex", "pattern": "^[A-Z0-9-]+$"},
        ],
    }


def spec_v2(allow_missing=False):
    """v2: adds a loyalty_id column and an email format check (new field).

    allow_missing=True makes it a backward-compatible upgrade that can still
    read old 4-column payloads (loyalty_id comes out as None).
    """
    s = spec_v1()
    s["parser"]["columns"].append("loyalty_id")
    if allow_missing:
        s["parser"]["allow_missing_columns"] = True
    s["field_mapping"]["loyalty_id"] = {"target": "loyalty_id", "type": "string"}
    s["validation"].append(
        {"field": "email", "rule": "regex", "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"})
    return s


def spec_bad():
    """Broken upgrade: expects comma delimiter, so pipe-delimited data fails."""
    s = spec_v1()
    s["parser"]["delimiter"] = ","
    return s


def spec_empty_prone():
    """Maps a column that real payloads don't carry -> all-blank canonical."""
    return {
        "parser": {"format": "delimited", "delimiter": "|", "trim": True,
                   "columns": ["order_id", "amount_raw", "currency", "email"]},
        "field_mapping": {
            "nonexistent_col": {"target": "ghost", "type": "string"},
        },
        "validation": [],
    }


def good_records(n, start=0):
    return [f"ORD-{1000+i}|{10+i}.50|USD|user{i}@example.com" for i in range(start, start + n)]


def v2_records(n, start=0):
    return [f"ORD-{1000+i}|{10+i}.50|USD|user{i}@example.com|LOY-{i}"
            for i in range(start, start + n)]


@pytest.fixture()
def db():
    database = Database(":memory:")
    yield database
    database.close()


@pytest.fixture()
def app(db):
    return create_app(db)


@pytest.fixture()
def source_id(db):
    return db.execute(
        "INSERT INTO sources(name, error_threshold, empty_threshold, min_sample,"
        " created_at) VALUES ('feed', 0.05, 0.20, 20, '2026-01-01T00:00:00Z')")


def register(db, name, version, spec, status="published"):
    vid = db.execute(
        "INSERT INTO plugin_versions(name, version, spec_json, status, created_at)"
        " VALUES (?,?,?,?, '2026-01-01T00:00:00Z')",
        (name, version, __import__("json").dumps(spec), "draft"))
    if status != "draft":
        db.execute("UPDATE plugin_versions SET status=? WHERE id=?", (status, vid))
    return vid


def bind_active(db, source_id, version_id):
    db.execute("UPDATE sources SET active_version_id=? WHERE id=?", (version_id, source_id))
    db.execute(
        "INSERT INTO bindings(source_id, version_id, role, bound_at)"
        " VALUES (?,?, 'active', '2026-01-01T00:00:00Z')", (source_id, version_id))


def upload(db, source_id, records, name="b1"):
    from app.db import dumps, now_iso
    bid = db.execute(
        "INSERT INTO batches(source_id, name, records_json, created_at) VALUES (?,?,?,?)",
        (source_id, name, dumps(records), now_iso()))
    return bid
