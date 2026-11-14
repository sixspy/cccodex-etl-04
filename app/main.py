from pathlib import Path

from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import BadSignature, URLSafeSerializer

from app.config import Config
from app.db import LabError, get_db, init_db
from app.table_flow import (
    add_table,
    board,
    bulk_extract,
    bulk_load,
    detail,
    extract_table,
    load_table,
    overview,
    reorder,
    set_policy,
)

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
serializer = URLSafeSerializer(Config.SECRET, salt="cc-etl-session")

app = FastAPI(title="cc-etl-lab")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


@app.exception_handler(LabError)
async def lab_error_handler(request: Request, exc: LabError):
    body = {"ok": False, "error": exc.detail, "code": exc.code}
    body.update(exc.payload)
    return JSONResponse(body, status_code=exc.status_code)


def _read_session(request: Request) -> dict:
    raw = request.cookies.get("etl_session")
    if not raw:
        return {}
    try:
        data = serializer.loads(raw)
        return data if isinstance(data, dict) else {}
    except BadSignature:
        return {}


def require_user(request: Request) -> str:
    user = _read_session(request).get("user")
    if user != Config.ADMIN_USER:
        raise HTTPException(status_code=401, detail="未登录")
    return user


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/")
def root(request: Request):
    if _read_session(request).get("user") == Config.ADMIN_USER:
        return RedirectResponse("/app/", status_code=302)
    return RedirectResponse("/login", status_code=302)


@app.get("/login")
def login_page():
    return FileResponse(STATIC / "login.html")


@app.get("/app/")
def spa_page(request: Request):
    if _read_session(request).get("user") != Config.ADMIN_USER:
        return RedirectResponse("/login", status_code=302)
    return FileResponse(STATIC / "index.html")


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    user = str(body.get("username", "")).strip()
    pwd = str(body.get("password", ""))
    if user != Config.ADMIN_USER or pwd != Config.ADMIN_PASSWORD:
        return JSONResponse({"ok": False, "error": "账号或密码错误"}, status_code=400)
    resp = JSONResponse({"ok": True, "user": user})
    resp.set_cookie(
        "etl_session",
        serializer.dumps({"user": user}),
        httponly=True,
        samesite="lax",
    )
    return resp


@app.post("/api/logout")
def api_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("etl_session")
    return resp


@app.get("/api/me")
def api_me(user: str = Depends(require_user)):
    return {"user": user, "lab": Config.LAB_NAME, "domain": "数据工程"}


# ---------------------------------------------------------------------------
# 表清单总览：每条管道带表清单、策略标题与顺序锁/待对齐摘要
# ---------------------------------------------------------------------------

@app.get("/api/pipelines")
def api_pipelines(user: str = Depends(require_user)):
    with get_db() as conn:
        items = overview(conn)
    return {"items": items}


@app.get("/api/pipelines/{pipeline_id}")
def api_pipeline_detail(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = detail(conn, pipeline_id)
    return item


# 顺序板：表状态、待对齐标记、策略标题、顺序锁与表序抢占记录。
@app.get("/api/pipelines/{pipeline_id}/board")
def api_pipeline_board(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        payload = board(conn, pipeline_id)
    return payload


# ---------------------------------------------------------------------------
# 跳表设置
# ---------------------------------------------------------------------------

@app.post("/api/pipelines/{pipeline_id}/policy")
def api_set_policy(pipeline_id: int, body: dict = Body(default={}),
                   user: str = Depends(require_user)):
    policy = str(body.get("policy") or body.get("skip_policy") or "").strip()
    note = body.get("note")
    with get_db() as conn:
        result = set_policy(conn, pipeline_id, policy, note)
        payload = board(conn, pipeline_id)
    return {"ok": True, **result, "tables": payload["tables"],
            "order_locks": payload["order_locks"], "races": payload["races"]}


# ---------------------------------------------------------------------------
# 表顺序调整与表清单维护
# ---------------------------------------------------------------------------

@app.post("/api/pipelines/{pipeline_id}/order")
def api_reorder(pipeline_id: int, body: dict = Body(default={}),
                user: str = Depends(require_user)):
    ordered = body.get("tables") or body.get("order") or body.get("ordered")
    with get_db() as conn:
        payload = reorder(conn, pipeline_id, ordered)
    return {"ok": True, **payload}


@app.post("/api/pipelines/{pipeline_id}/tables")
def api_add_table(pipeline_id: int, body: dict = Body(default={}),
                  user: str = Depends(require_user)):
    with get_db() as conn:
        payload = add_table(conn, pipeline_id, body.get("name"))
    return {"ok": True, **payload}


# ---------------------------------------------------------------------------
# 抽取 / 落库：按表操作，序位由服务端裁定
# ---------------------------------------------------------------------------

@app.post("/api/pipelines/{pipeline_id}/tables/{ref}/extract")
def api_table_extract(pipeline_id: int, ref: str,
                      user: str = Depends(require_user)):
    result = extract_table(pipeline_id, ref, user)
    with get_db() as conn:
        payload = board(conn, pipeline_id)
    return {"ok": True, **result,
            "tables": payload["tables"],
            "order_locks": payload["order_locks"],
            "races": payload["races"]}


@app.post("/api/pipelines/{pipeline_id}/tables/{ref}/load")
def api_table_load(pipeline_id: int, ref: str,
                   user: str = Depends(require_user)):
    result = load_table(pipeline_id, ref, user)
    with get_db() as conn:
        payload = board(conn, pipeline_id)
    return {"ok": True, **result,
            "tables": payload["tables"],
            "order_locks": payload["order_locks"],
            "races": payload["races"]}


# 兼容基线入口：整轮按声明顺序抽表 / 落表。
@app.post("/api/pipelines/{pipeline_id}/extract")
def api_extract(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        pipeline = bulk_extract(conn, pipeline_id)
    with get_db() as conn:
        payload = board(conn, pipeline_id)
    return {"ok": True, **pipeline,
            "tables": payload["tables"],
            "order_locks": payload["order_locks"],
            "races": payload["races"],
            "policy": payload["policy"]}


@app.post("/api/pipelines/{pipeline_id}/load")
def api_load(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        pipeline = bulk_load(conn, pipeline_id)
    with get_db() as conn:
        payload = board(conn, pipeline_id)
    return {"ok": True, **pipeline,
            "tables": payload["tables"],
            "order_locks": payload["order_locks"],
            "races": payload["races"]}


def create_app() -> FastAPI:
    return app
