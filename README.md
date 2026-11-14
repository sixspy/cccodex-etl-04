# cc-etl-lab

CC_Codex 二期（26.11）**数据工程**产品线：**抽取转换加载台**（FastAPI + Vue 3 SPA + SQLite）。

与 `cc-data-lab`（Flask + HTMX 采集控制台）错开：本线面向 **ETL 管道编排 / 抽取落库**，前端为 Vue 单页，暖色仓运风格。

## 领域

- **所属领域**：数据工程  
- **子方向**：抽取-转换-加载 / 数据采集与落库  

## 技术栈

| 层 | 选型 |
|----|------|
| 后端 | Python 3.12、FastAPI、Uvicorn、SQLite |
| 前端 | Vue 3（CDN Composition）、双栏管道台 |
| 风格 | 暖琥珀仓运底、圆角强调、等宽源地址 |
| 测试 | pytest + httpx |

## 基线能力

登录、管道列表、管道详情、执行抽取、执行落库。

## 同源多表抽取顺序锁

同一管道下的多张表按声明顺序（`pipeline_tables.seq`）抽取，序位由服务端裁定，
前端只提交表 ID，不提交序号。

- **表清单总览 / 管道详情**：每条管道带表清单、策略标题、待对齐与顺序锁计数；
  详情可读顺序锁记录与表序抢占记录。
- **顺序板**：逐表列出序位、状态、`顺序锁` 与 `待对齐` 两种独立标记、缺上游；
  下方分别列出「顺序锁记录」与「表序抢占记录」，详情接口同样可读。
- **跳表设置**：选定策略后写入口径说明并锁到当日（`policy_date`）。
  - `禁止跳表 · 顺序锁`：上游未齐时禁止抽下游，写顺序锁，直到上游全部抽取完成。
  - `允许跳表 · 待对齐`：上游未齐时允许抽下游，标待对齐，落库被拒；上游齐后自动对齐。
- **服务端裁定**：
  - 空表名禁止入清单；表名重复亦拒绝。
  - 有表仍在抽取流程（抽取中 / 已抽取待加载 / 待对齐）时禁止改表顺序。
  - 同刻点击不同表时，声明序最前的一张成功，其余登记「表序争抢失败」。
  - 说明与所选策略口径不一致时，抽取返回 `policy_drifted`（跳表策略偏了）。

错误码：`order_lock`、`align_load_blocked`、`order_race_lost`、
`order_locked_in_flow`、`empty_table_name`、`policy_drifted`。


## 本地

```powershell
cd claude-workspace\cc-etl-lab
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest
python run.py
```

http://127.0.0.1:6000/login · **admin / 123456**

## Docker

```powershell
docker compose up -d --build
```

端口 **6000**。冒烟：`python ..\scripts\_smoke_lab_access.py --dir .`
