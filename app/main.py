"""HTTP API + static UI for the parser plugin system."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import compat, monitor, pipeline
from .db import Database, dumps, loads, now_iso
from .plugins import SpecError, diff_specs, validate_spec

STATIC_DIR = Path(__file__).parent / "static"


# ------------------------------------------------------------------ schemas
class SourceIn(BaseModel):
    name: str
    error_threshold: float = 0.05
    empty_threshold: float = 0.20
    min_sample: int = 20


class VersionIn(BaseModel):
    name: str
    version: str
    spec: dict[str, Any]


class PublishIn(BaseModel):
    source_id: int
    gray_percent: int = Field(0, ge=0, le=100)
    force: bool = False


class BatchIn(BaseModel):
    source_id: int
    name: str
    records: list[Any]


class ReplayIn(BaseModel):
    version_id: int


class GrayIn(BaseModel):
    percent: int = Field(..., ge=0, le=100)


def create_app(db: Database | None = None) -> FastAPI:
    app = FastAPI(title="Parser Plugin System")
    app.state.db = db or Database(str(Path(__file__).parent.parent / "parser_plugins.db"))

    def get_db() -> Database:
        return app.state.db

    # ------------------------------------------------------------- sources
    @app.post("/api/sources")
    def create_source(body: SourceIn):
        db = get_db()
        try:
            sid = db.execute(
                "INSERT INTO sources(name, error_threshold, empty_threshold,"
                " min_sample, created_at) VALUES (?,?,?,?,?)",
                (body.name, body.error_threshold, body.empty_threshold,
                 body.min_sample, now_iso()))
        except Exception as exc:
            raise HTTPException(409, f"source exists or invalid: {exc}")
        return {"id": sid}

    @app.get("/api/sources")
    def list_sources():
        db = get_db()
        out = []
        for s in db.query("SELECT * FROM sources ORDER BY id"):
            item = dict(s)
            for role, key in (("active", "active_version_id"),
                              ("gray", "gray_version_id")):
                vid = s[key]
                if vid:
                    v = db.query_one(
                        "SELECT name, version, status FROM plugin_versions WHERE id=?",
                        (vid,))
                    item[f"{role}_version"] = v
                    item[f"{role}_metrics"] = pipeline.version_metrics(db, s["id"], vid)
                else:
                    item[f"{role}_version"] = None
            out.append(item)
        return out

    @app.get("/api/sources/{source_id}/metrics")
    def source_metrics(source_id: int):
        db = get_db()
        rows = db.query(
            "SELECT DISTINCT version_id FROM runs WHERE source_id=?", (source_id,))
        return [
            {"version_id": r["version_id"],
             **pipeline.version_metrics(db, source_id, r["version_id"])}
            for r in rows
        ]

    @app.post("/api/sources/{source_id}/gray")
    def set_gray(source_id: int, body: GrayIn):
        try:
            monitor.set_gray_percent(get_db(), source_id, body.percent)
        except (KeyError, ValueError) as exc:
            raise HTTPException(400, str(exc))
        return {"ok": True}

    @app.post("/api/sources/{source_id}/rollback")
    def manual_rollback(source_id: int):
        try:
            event_id = monitor.rollback(get_db(), source_id, trigger="manual",
                                        reason="manual rollback via UI/API")
        except KeyError as exc:
            raise HTTPException(404, str(exc))
        if event_id is None:
            raise HTTPException(409, "nothing to roll back")
        return {"rollback_event_id": event_id}

    # ------------------------------------------------------------- versions
    @app.post("/api/plugins/versions")
    def register_version(body: VersionIn):
        db = get_db()
        try:
            validate_spec(body.spec)
        except SpecError as exc:
            raise HTTPException(422, f"invalid spec: {exc}")
        try:
            vid = db.execute(
                "INSERT INTO plugin_versions(name, version, spec_json, created_at)"
                " VALUES (?,?,?,?)",
                (body.name, body.version, dumps(body.spec), now_iso()))
        except Exception:
            raise HTTPException(409, f"{body.name} {body.version} already registered")
        return {"id": vid}

    @app.get("/api/plugins/versions")
    def list_versions():
        return get_db().query(
            "SELECT id, name, version, status, created_at, published_at"
            " FROM plugin_versions ORDER BY name, id")

    @app.get("/api/plugins/versions/{version_id}")
    def get_version(version_id: int):
        row = get_db().query_one("SELECT * FROM plugin_versions WHERE id=?",
                                 (version_id,))
        if not row:
            raise HTTPException(404, "version not found")
        row["spec"] = loads(row.pop("spec_json"))
        return row

    @app.get("/api/plugins/compare")
    def compare_versions(a: int, b: int):
        db = get_db()
        rows = {}
        for vid in (a, b):
            row = db.query_one("SELECT * FROM plugin_versions WHERE id=?", (vid,))
            if not row:
                raise HTTPException(404, f"version {vid} not found")
            rows[vid] = row
        spec_a, spec_b = (loads(rows[a]["spec_json"]), loads(rows[b]["spec_json"]))
        metrics = {}
        for vid in (a, b):
            sources = db.query(
                "SELECT DISTINCT source_id FROM runs WHERE version_id=?", (vid,))
            metrics[vid] = [
                {"source_id": s["source_id"],
                 **pipeline.version_metrics(db, s["source_id"], vid)}
                for s in sources
            ]
        samples = db.query(
            "SELECT d.* FROM diff_samples d JOIN compat_reports r ON r.id=d.report_id"
            " WHERE (r.baseline_version_id=? AND r.candidate_version_id=?)"
            "    OR (r.baseline_version_id=? AND r.candidate_version_id=?) LIMIT 50",
            (a, b, b, a))
        return {
            "a": {"id": a, "name": rows[a]["name"], "version": rows[a]["version"],
                  "status": rows[a]["status"]},
            "b": {"id": b, "name": rows[b]["name"], "version": rows[b]["version"],
                  "status": rows[b]["status"]},
            "spec_diff": diff_specs(spec_a, spec_b),
            "metrics": metrics,
            "diff_samples": [
                {**s, "baseline": loads(s.pop("baseline_json")),
                 "candidate": loads(s.pop("candidate_json"))}
                for s in samples
            ],
        }

    # ------------------------------------------------- compat + publishing
    @app.post("/api/plugins/versions/{version_id}/compatibility")
    def compatibility(version_id: int, source_id: int, sample_size: int = 200):
        try:
            report_id = compat.generate_report(get_db(), source_id, version_id,
                                               sample_size)
        except KeyError as exc:
            raise HTTPException(404, str(exc))
        return {"report_id": report_id}

    @app.get("/api/compat-reports/{report_id}")
    def get_report(report_id: int):
        db = get_db()
        row = db.query_one("SELECT * FROM compat_reports WHERE id=?", (report_id,))
        if not row:
            raise HTTPException(404, "report not found")
        row["summary"] = loads(row.pop("summary_json"))
        row["diff_samples"] = [
            {**s, "baseline": loads(s.pop("baseline_json")),
             "candidate": loads(s.pop("candidate_json"))}
            for s in db.query(
                "SELECT record_key, baseline_json, candidate_json FROM diff_samples"
                " WHERE report_id=? LIMIT 50", (report_id,))
        ]
        return row

    @app.post("/api/plugins/versions/{version_id}/publish")
    def publish_version(version_id: int, body: PublishIn):
        try:
            monitor.publish(get_db(), body.source_id, version_id,
                            gray_percent=body.gray_percent, force=body.force)
        except KeyError as exc:
            raise HTTPException(404, str(exc))
        except monitor.PublishError as exc:
            raise HTTPException(409, str(exc))
        return {"ok": True}

    # ------------------------------------------------------------- batches
    @app.post("/api/batches")
    def upload_batch(body: BatchIn):
        db = get_db()
        if not db.query_one("SELECT id FROM sources WHERE id=?", (body.source_id,)):
            raise HTTPException(404, "source not found")
        batch_id = db.execute(
            "INSERT INTO batches(source_id, name, records_json, created_at)"
            " VALUES (?,?,?,?)",
            (body.source_id, body.name, dumps(body.records), now_iso()))
        try:
            run_ids = pipeline.process_batch(db, batch_id)
        except ValueError as exc:
            raise HTTPException(409, str(exc))
        event_id = monitor.evaluate_source(db, body.source_id)
        return {"batch_id": batch_id, "run_ids": run_ids,
                "auto_rollback_event_id": event_id}

    @app.get("/api/batches")
    def list_batches(source_id: int | None = None):
        db = get_db()
        sql = "SELECT * FROM batches"
        params: tuple = ()
        if source_id:
            sql += " WHERE source_id=?"
            params = (source_id,)
        sql += " ORDER BY id DESC LIMIT 100"
        batches = db.query(sql, params)
        for b in batches:
            b["record_count"] = len(loads(b["records_json"]))
            b.pop("records_json")
            b["runs"] = db.query(
                "SELECT id, version_id, kind, stats_json, created_at FROM runs"
                " WHERE batch_id=? ORDER BY id", (b["id"],))
            for r in b["runs"]:
                r["stats"] = loads(r.pop("stats_json"))
        return batches

    @app.post("/api/batches/{batch_id}/replay")
    def replay(batch_id: int, body: ReplayIn):
        db = get_db()
        try:
            run_id = pipeline.replay_batch(db, batch_id, body.version_id)
        except KeyError as exc:
            raise HTTPException(404, str(exc))
        batch = db.query_one("SELECT source_id FROM batches WHERE id=?", (batch_id,))
        event_id = monitor.evaluate_source(db, batch["source_id"])
        return {"run_id": run_id, "auto_rollback_event_id": event_id}

    @app.get("/api/runs/{run_id}/results")
    def run_results(run_id: int):
        db = get_db()
        run = db.query_one("SELECT * FROM runs WHERE id=?", (run_id,))
        if not run:
            raise HTTPException(404, "run not found")
        run["stats"] = loads(run.pop("stats_json"))
        run["results"] = [
            {**r, "canonical": loads(r.pop("canonical_json")),
             "extras": loads(r.pop("extras_json")),
             "errors": loads(r.pop("errors_json"))}
            for r in db.query(
                "SELECT record_index, record_key, status, canonical_json,"
                " extras_json, errors_json FROM results WHERE run_id=?"
                " ORDER BY record_index", (run_id,))
        ]
        return run

    # ------------------------------------------------------------- events
    @app.get("/api/rollback-events")
    def rollback_events(source_id: int | None = None):
        db = get_db()
        sql = "SELECT * FROM rollback_events"
        params: tuple = ()
        if source_id:
            sql += " WHERE source_id=?"
            params = (source_id,)
        sql += " ORDER BY id DESC LIMIT 100"
        events = db.query(sql, params)
        for e in events:
            e["metrics"] = loads(e.pop("metrics_json"))
            e["preserved_run_ids"] = loads(e.pop("preserved_run_ids_json"))
            e["diff_samples"] = [
                {**s, "baseline": loads(s.pop("baseline_json")),
                 "candidate": loads(s.pop("candidate_json"))}
                for s in db.query(
                    "SELECT record_key, baseline_json, candidate_json FROM diff_samples"
                    " WHERE rollback_event_id=? LIMIT 50", (e["id"],))
            ]
        return events

    # ------------------------------------------------------------- static
    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
