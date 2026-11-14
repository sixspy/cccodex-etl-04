"""同源多表抽取顺序锁：锁、跳表策略、表序争抢、空表名与改序冻结。"""

import threading


def board(client, pid):
    return client.get(f"/api/pipelines/{pid}/board").json()


def tables(client, pid):
    return board(client, pid)["tables"]


def test_table_manifest_and_order_are_server_decided(authed, pipeline_id):
    """表清单总览带表序与策略标题，顺序板与服务端 seq 一致。"""
    overview = authed.get("/api/pipelines").json()["items"]
    item = next(i for i in overview if i["id"] == pipeline_id)
    assert item["summary"]["table_count"] >= 2
    assert item["policy"]["policy_title"]

    view = board(authed, pipeline_id)
    assert view["policy"]["policy_title"] in ("禁止跳表 · 顺序锁", "允许跳表 · 待对齐")
    assert [t["seq"] for t in view["tables"]] == list(range(1, len(view["tables"]) + 1))
    assert view["tables"][0]["name"] == "ods_orders_raw"


def test_block_policy_writes_order_lock_and_forbids_skip(authed, pipeline_id):
    """block 策略：上游未齐时禁止抽下游，并写顺序锁。"""
    r = authed.post(f"/api/pipelines/{pipeline_id}/tables/2/extract")
    assert r.status_code == 409
    data = r.json()
    assert data["code"] == "order_lock"
    assert data["missing_upstream"] == ["ods_orders_raw"]
    assert data["order_lock"]["label"] == "顺序锁"

    view = board(authed, pipeline_id)
    assert any(lock["table_name"] == "dwd_orders_detail"
               for lock in view["order_locks"])
    row = next(t for t in view["tables"] if t["name"] == "dwd_orders_detail")
    assert row["order_locked"] is True
    assert row["status"] == "待抽取"
    # 顺序锁与待对齐是两种不同标记。
    assert row["align_pending"] is False


def test_lock_clears_after_upstream_extracted_and_order_flows(authed, pipeline_id):
    """上游抽取完成后，下游可按声明顺序继续；锁记录保留在板上。"""
    authed.post(f"/api/pipelines/{pipeline_id}/tables/2/extract")
    r1 = authed.post(f"/api/pipelines/{pipeline_id}/tables/1/extract")
    assert r1.status_code == 200
    assert r1.json()["table"]["status"] == "已抽取待加载"

    r2 = authed.post(f"/api/pipelines/{pipeline_id}/tables/2/extract")
    assert r2.status_code == 200
    assert r2.json()["table"]["status"] == "已抽取待加载"

    view = board(authed, pipeline_id)
    assert [t["status"] for t in view["tables"][:2]] == [
        "已抽取待加载", "已抽取待加载"
    ]
    assert len(view["order_locks"]) >= 1


def test_allow_pending_policy_marks_alignment_and_blocks_load(authed, pipeline_id):
    """allow_pending：允许抽下游但标待对齐，且禁止落库。"""
    r = authed.post(f"/api/pipelines/{pipeline_id}/policy",
                    json={"policy": "allow_pending"})
    assert r.status_code == 200
    assert r.json()["policy"]["skip_policy"] == "allow_pending"

    ex = authed.post(f"/api/pipelines/{pipeline_id}/tables/3/extract")
    assert ex.status_code == 200
    payload = ex.json()
    assert payload["table"]["status"] == "待对齐"
    assert payload["align_pending"] is True
    assert payload["missing_upstream"]

    ld = authed.post(f"/api/pipelines/{pipeline_id}/tables/3/load")
    assert ld.status_code == 409
    assert ld.json()["code"] == "align_load_blocked"

    row = next(t for t in tables(authed, pipeline_id) if t["name"] == "dws_orders_day")
    assert row["align_pending"] is True
    assert row["order_locked"] is False
    assert row["align_label"] == "待对齐"


def test_aligned_table_released_after_upstream_extracts(authed, pipeline_id):
    """上游表全部抽取完成后，待对齐的下游自动对齐为已抽取待加载。"""
    authed.post(f"/api/pipelines/{pipeline_id}/policy",
                json={"policy": "allow_pending"})
    authed.post(f"/api/pipelines/{pipeline_id}/tables/3/extract")
    authed.post(f"/api/pipelines/{pipeline_id}/tables/1/extract")
    ex = authed.post(f"/api/pipelines/{pipeline_id}/tables/2/extract")
    assert ex.status_code == 200
    assert ex.json()["released"] == ["dws_orders_day"]
    row = next(t for t in tables(authed, pipeline_id) if t["name"] == "dws_orders_day")
    assert row["status"] == "已抽取待加载"
    assert row["align_pending"] is False


def test_policy_drift_blocks_extract(authed, pipeline_id):
    """说明与当日口径不一致时，点抽取返回「跳表策略偏了」。"""
    r = authed.post(
        f"/api/pipelines/{pipeline_id}/policy",
        json={"policy": "block", "note": "随便写点什么，不含口径关键词"},
    )
    assert r.status_code == 200
    assert r.json()["policy"]["policy_mismatch"] is True

    ex = authed.post(f"/api/pipelines/{pipeline_id}/tables/1/extract")
    assert ex.status_code == 409
    assert ex.json()["code"] == "policy_drifted"
    assert "跳表策略偏了" in ex.json()["error"]


def test_policy_note_written_to_spec_on_selection(authed, pipeline_id):
    r = authed.post(f"/api/pipelines/{pipeline_id}/policy",
                    json={"policy": "allow_pending"})
    data = r.json()
    assert data["policy"]["policy_mismatch"] is False
    assert "待对齐" in data["policy"]["policy_note"]
    assert "允许" in data["policy"]["policy_note"]
    assert data["policy"]["policy_day"]


def test_empty_table_name_rejected(authed, pipeline_id):
    for name in ("", "   ", "\n\t"):
        r = authed.post(f"/api/pipelines/{pipeline_id}/tables", json={"name": name})
        assert r.status_code == 400
        assert r.json()["code"] == "empty_table_name"


def test_reorder_forbidden_while_table_in_flow(authed, pipeline_id):
    authed.post(f"/api/pipelines/{pipeline_id}/tables/1/extract")
    view = board(authed, pipeline_id)
    ids = [t["id"] for t in view["tables"]][::-1]
    r = authed.post(f"/api/pipelines/{pipeline_id}/order", json={"tables": ids})
    assert r.status_code == 409
    assert r.json()["code"] == "order_locked_in_flow"
    # 序没被动过。
    assert [t["seq"] for t in tables(authed, pipeline_id)] == [1, 2, 3]


def test_reorder_works_when_idle(authed, pipeline_id):
    ids = [t["id"] for t in tables(authed, pipeline_id)]
    r = authed.post(f"/api/pipelines/{pipeline_id}/order",
                    json={"tables": ids[::-1]})
    assert r.status_code == 200
    names = [t["name"] for t in r.json()["tables"]]
    assert names[0] == "dws_orders_day"


def test_same_moment_only_first_table_wins(client, pipeline_id):
    """两人同刻点不同表：只许声明序最前的一张成功，其它记表序争抢失败。"""
    client.post("/api/login", json={"username": "admin", "password": "123456"})
    ids = [t["id"] for t in tables(client, pipeline_id)]
    results = {}
    barrier = threading.Barrier(len(ids))

    def hit(table_id):
        barrier.wait()
        r = client.post(f"/api/pipelines/{pipeline_id}/tables/{table_id}/extract")
        results[table_id] = (r.status_code, r.json())

    threads = [threading.Thread(target=hit, args=(tid,)) for tid in ids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [tid for tid, (sc, _) in results.items() if sc == 200]
    assert len(winners) == 1
    assert winners[0] == ids[0]
    losers = [data for tid, (sc, data) in results.items() if tid != ids[0]]
    for data in losers:
        assert data["code"] == "order_race_lost"
        assert "同刻表序争抢失败" in data["error"]

    view = board(client, pipeline_id)
    raced_names = {r["table_name"] for r in view["races"]}
    assert raced_names <= {t["name"] for t in view["tables"] if t["seq"] > 1}
    assert view["races"] and all(
        r["won_table_name"] == "ods_orders_raw" for r in view["races"]
    )
    assert next(t for t in view["tables"] if t["id"] == ids[0])["status"] == "已抽取待加载"


def test_board_separates_order_lock_from_alignment(authed, pipeline_id):
    """页面数据能分清顺序锁与待对齐：两类记录、两种标记互不混淆。"""
    authed.post(f"/api/pipelines/{pipeline_id}/tables/2/extract")
    locked = next(t for t in tables(authed, pipeline_id)
                  if t["name"] == "dwd_orders_detail")
    assert locked["order_lock_label"] == "顺序锁"
    assert locked["align_label"] == ""

    authed.post(f"/api/pipelines/{pipeline_id}/policy",
                json={"policy": "allow_pending"})
    authed.post(f"/api/pipelines/{pipeline_id}/tables/3/extract")
    aligned = next(t for t in tables(authed, pipeline_id)
                   if t["name"] == "dws_orders_day")
    assert aligned["align_label"] == "待对齐"
    assert aligned["order_lock_label"] == ""

    view = board(authed, pipeline_id)
    assert view["order_locks"][0]["label"] == "顺序锁"
    assert view["policy"]["policy_title"] == "允许跳表 · 待对齐"


def test_detail_reads_locks_and_races(authed, pipeline_id):
    authed.post(f"/api/pipelines/{pipeline_id}/tables/2/extract")
    detail = authed.get(f"/api/pipelines/{pipeline_id}").json()
    assert detail["order_locks"]
    assert detail["order_locks"][0]["missing_upstream"] == "ods_orders_raw"
    assert "races" in detail
    assert detail["summary"]["order_locked"] >= 1



def test_bulk_extract_flows_in_declared_order(authed, pipeline_id):
    """整轮抽取按声明顺序把表流到已抽取待加载，并如实回报各表状态。"""
    r = authed.post(f"/api/pipelines/{pipeline_id}/extract")
    assert r.status_code == 200
    body = r.json()
    assert [t["name"] for t in body["tables"]] == [
        "ods_orders_raw", "dwd_orders_detail", "dws_orders_day"
    ]
    assert all(t["status"] == "已抽取待加载" for t in body["tables"])
    assert body["status"] == "已抽取待加载"

    # 待对齐的表不参与整轮流转，也不会被谎报成已抽取。
    other = authed.get("/api/pipelines").json()["items"][1]["id"]
    authed.post(f"/api/pipelines/{other}/policy",
                json={"policy": "allow_pending"})
    authed.post(f"/api/pipelines/{other}/tables/6/extract")
    authed.post(f"/api/pipelines/{other}/policy", json={"policy": "block"})
    r = authed.post(f"/api/pipelines/{other}/extract")
    assert r.status_code == 200
    aligned = next(t for t in r.json()["tables"] if t["name"] == "dws_users_tag")
    assert aligned["status"] == "已抽取待加载"

