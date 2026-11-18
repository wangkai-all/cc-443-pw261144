"""End-to-end API flow via FastAPI TestClient."""
from fastapi.testclient import TestClient

from tests.conftest import good_records, spec_bad, spec_v1, spec_v2  # noqa: F401


def test_full_http_flow(app):
    c = TestClient(app)

    src = c.post("/api/sources", json={
        "name": "feed", "error_threshold": 0.05, "empty_threshold": 0.2,
        "min_sample": 10}).json()
    sid = src["id"]

    v1 = c.post("/api/plugins/versions", json={
        "name": "orders", "version": "1.0.0", "spec": spec_v1()}).json()["id"]
    v2 = c.post("/api/plugins/versions", json={
        "name": "orders", "version": "1.1.0",
        "spec": spec_v2(allow_missing=True)}).json()["id"]

    # publish requires a compat report first
    r = c.post(f"/api/plugins/versions/{v1}/publish", json={"source_id": sid})
    assert r.status_code == 409
    rep = c.post(f"/api/plugins/versions/{v1}/compatibility?source_id={sid}").json()
    assert c.get(f"/api/compat-reports/{rep['report_id']}").json()["verdict"] in \
        {"pass", "warn"}
    assert c.post(f"/api/plugins/versions/{v1}/publish",
                  json={"source_id": sid}).status_code == 200

    # upload + process
    r = c.post("/api/batches", json={
        "source_id": sid, "name": "b1", "records": good_records(30)}).json()
    assert r["auto_rollback_event_id"] is None
    run_id = r["run_ids"][0]
    results = c.get(f"/api/runs/{run_id}/results").json()
    assert results["stats"]["ok"] == 30

    # v2 as gray
    c.post(f"/api/plugins/versions/{v2}/compatibility?source_id={sid}")
    assert c.post(f"/api/plugins/versions/{v2}/publish",
                  json={"source_id": sid, "gray_percent": 50}).status_code == 200
    r = c.post("/api/batches", json={
        "source_id": sid, "name": "b2", "records": good_records(40, 100)}).json()
    assert len(r["run_ids"]) == 2  # active + gray runs

    # gray slider
    assert c.post(f"/api/sources/{sid}/gray", json={"percent": 10}).status_code == 200
    sources = c.get("/api/sources").json()
    assert sources[0]["gray_percent"] == 10

    # compare endpoint
    cmp = c.get(f"/api/plugins/compare?a={v1}&b={v2}").json()
    assert cmp["spec_diff"]["mapping_added"][0]["raw"] == "loyalty_id"
    assert cmp["metrics"]

    # replay
    batches = c.get("/api/batches").json()
    r = c.post(f"/api/batches/{batches[0]['id']}/replay",
               json={"version_id": v1}).json()
    assert r["run_id"]

    # broken version -> auto rollback via batch upload
    bad = c.post("/api/plugins/versions", json={
        "name": "orders", "version": "2.0.0", "spec": spec_bad()}).json()["id"]
    c.post(f"/api/plugins/versions/{bad}/compatibility?source_id={sid}")
    assert c.post(f"/api/plugins/versions/{bad}/publish",
                  json={"source_id": sid, "force": True}).status_code == 200
    r = c.post("/api/batches", json={
        "source_id": sid, "name": "b3", "records": good_records(30, 200)}).json()
    assert r["auto_rollback_event_id"] is not None

    events = c.get("/api/rollback-events").json()
    assert events[0]["trigger"] == "auto"
    assert events[0]["preserved_run_ids"]
    assert events[0]["diff_samples"]
    # source is back on the previous healthy active version
    src_after = c.get("/api/sources").json()[0]
    assert src_after["active_version"]["version"] == "1.0.0"

    # manual rollback of remaining version
    r = c.post(f"/api/sources/{sid}/rollback")
    assert r.status_code == 200


def test_invalid_spec_rejected(app):
    c = TestClient(app)
    r = c.post("/api/plugins/versions", json={
        "name": "x", "version": "1", "spec": {"parser": {"format": "nope"}}})
    assert r.status_code == 422


def test_duplicate_version_rejected(app):
    c = TestClient(app)
    body = {"name": "x", "version": "1", "spec": spec_v1()}
    assert c.post("/api/plugins/versions", json=body).status_code == 200
    assert c.post("/api/plugins/versions", json=body).status_code == 409
