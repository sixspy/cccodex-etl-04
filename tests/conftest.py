import pytest
from fastapi.testclient import TestClient

from app import table_flow
from app.db import init_db
from app.main import app
from app.table_flow import reset_runtime_state


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_PATH", str(db))
    # 同刻窗口与占用时长缩短，测试快且稳定。
    monkeypatch.setattr(table_flow, "COALESCE_SECONDS", 0.05)
    monkeypatch.setattr(table_flow, "HOLD_SECONDS", 0.12)
    reset_runtime_state()
    init_db(str(db))
    with TestClient(app) as c:
        yield c
    reset_runtime_state()


@pytest.fixture()
def authed(client):
    r = client.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200
    return client


@pytest.fixture()
def pipeline_id(authed):
    items = authed.get("/api/pipelines").json()["items"]
    return items[0]["id"]
