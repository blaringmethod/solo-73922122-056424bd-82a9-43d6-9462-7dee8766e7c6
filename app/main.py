"""FastAPI 应用：疾控汇总发布安全审查台。

页面：
- GET /        内部审查台（导入、预览、发布；可见原始格值）
- GET /public  公开页（仅人数与获准汇总，无格内检出数）

接口：
- POST /api/import     导入调查 CSV（仅允许一次，导入后不可更换）
- GET  /api/admin/state 内部状态（含格内检出数）
- POST /api/preview    预览交集检出总数并做安全评估
- POST /api/publish    确认发布（按最新公开记录重核后原子写入）
- GET  /api/public     公开数据（人数 + 获准汇总）
"""

import asyncio
import csv
import io
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .database import (
    AlreadyImported,
    DatabaseBusy,
    get_cells,
    get_public_snapshot,
    get_publications,
    init_db,
    insert_cells_once,
    publish_guarded,
)
from .solver import evaluate

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).resolve().parent / "static"
SAMPLE_CSV = BASE_DIR / "sample_survey.csv"

MAX_CELLS = 16
MIN_POPULATION = 5

# 仅用于演示/日志区分两台实例；不影响审查逻辑。
INSTANCE_LABEL = os.environ.get("INSTANCE_LABEL", "default")
# 另一台实例的浏览器可达地址（用于页面上演示并行确认）。
PEER_URL = os.environ.get("PEER_URL", "")


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(title="疾控汇总发布安全审查台", lifespan=lifespan)

# 演示用：允许两台实例的网页互相发起请求（仅本机演示场景，生产应限制来源）。
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# 说明：发布互斥不再依赖进程内锁。确认发布在 SQLite 的
# BEGIN IMMEDIATE 写事务内重读最新公开记录并重核，多台服务实例
# 各自独立连接同一库文件时也天然串行，见 database.publish_guarded()。


# ---------------- CSV 导入 ----------------

_HEADER_ALIASES = {
    "district": {"分区", "district", "地区", "区域"},
    "age_group": {"年龄组", "age_group", "agegroup", "age"},
    "population": {"人数", "population", "pop"},
    "detected": {"检出数", "detected", "count"},
}


def parse_survey_csv(text: str) -> list[dict]:
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(str(cell).strip() for cell in r)]
    if not rows:
        raise ValueError("CSV 为空")
    header = [str(h).strip().lstrip("﻿").lower() for h in rows[0]]
    col: dict[str, int] = {}
    for field, aliases in _HEADER_ALIASES.items():
        aliases = {a.lower() for a in aliases}
        for idx, h in enumerate(header):
            if h in aliases:
                col[field] = idx
                break
        if field not in col:
            raise ValueError(f"缺少必需列：{field}（可用表头：{sorted(aliases)}）")

    out, seen = [], set()
    for lineno, r in enumerate(rows[1:], start=2):
        try:
            district = r[col["district"]].strip()
            age = r[col["age_group"]].strip()
            pop_s = r[col["population"]].strip()
            det_s = r[col["detected"]].strip()
        except IndexError:
            raise ValueError(f"第 {lineno} 行列数不足")
        if not district or not age:
            raise ValueError(f"第 {lineno} 行分区或年龄组为空")
        try:
            pop = int(pop_s)
        except ValueError:
            raise ValueError(f"第 {lineno} 行人数不是整数：{pop_s!r}")
        try:
            det = int(det_s)
        except ValueError:
            raise ValueError(f"第 {lineno} 行检出数不是整数：{det_s!r}")
        if pop < MIN_POPULATION:
            raise ValueError(f"第 {lineno} 行人数 {pop} 小于下限 {MIN_POPULATION}")
        if not (0 <= det <= pop):
            raise ValueError(f"第 {lineno} 行检出数 {det} 不在 0~人数({pop}) 范围内")
        key = (district, age)
        if key in seen:
            raise ValueError(f"组合重复：{district} / {age}")
        seen.add(key)
        out.append(
            {"district": district, "age_group": age, "population": pop, "detected": det}
        )
    if not out:
        raise ValueError("没有数据行")
    if len(out) > MAX_CELLS:
        raise ValueError(f"格数 {len(out)} 超过上限 {MAX_CELLS}")
    return out


# ---------------- 请求模型 ----------------


class Selection(BaseModel):
    districts: list[str] = Field(default_factory=list)
    age_groups: list[str] = Field(default_factory=list)


# ---------------- 工具 ----------------


def _match(cells: list[dict], sel: Selection) -> tuple[list[dict], int]:
    known_d = {c["district"] for c in cells}
    known_a = {c["age_group"] for c in cells}
    bad_d = [d for d in sel.districts if d not in known_d]
    bad_a = [a for a in sel.age_groups if a not in known_a]
    if bad_d or bad_a:
        raise HTTPException(
            400, f"未知的分区或年龄组：{bad_d + bad_a}"
        )
    ds, ags = set(sel.districts), set(sel.age_groups)
    matched = [c for c in cells if c["district"] in ds and c["age_group"] in ags]
    if not matched:
        raise HTTPException(400, "选择为空或交集不含任何格")
    return matched, sum(c["detected"] for c in matched)


# ---------------- 页面 ----------------


@app.get("/", include_in_schema=False)
def index_page():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/public", include_in_schema=False)
def public_page():
    return FileResponse(STATIC_DIR / "public.html")


@app.get("/sample_survey.csv", include_in_schema=False)
def sample_csv():
    return FileResponse(SAMPLE_CSV, media_type="text/csv")


# ---------------- 接口 ----------------


@app.post("/api/import", status_code=201)
async def import_csv(file: UploadFile = File(...)):
    if get_cells():
        raise HTTPException(409, "调查数据已导入，不可更换")
    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(400, "文件编码须为 UTF-8")
    try:
        rows = parse_survey_csv(text)
    except ValueError as e:
        raise HTTPException(400, str(e))
    try:
        insert_cells_once(rows)
    except AlreadyImported:
        # 另一进程已先导入：本次整体回滚，先到先得。
        raise HTTPException(409, "调查数据已导入，不可更换")
    except DatabaseBusy:
        raise HTTPException(503, "数据库繁忙，导入未写入，请重试")
    return {
        "imported": len(rows),
        "districts": sorted({r["district"] for r in rows}),
        "age_groups": sorted({r["age_group"] for r in rows}),
    }


@app.get("/api/admin/state")
def admin_state():
    cells = get_cells()
    return {
        "instance": INSTANCE_LABEL,
        "peer_url": PEER_URL,
        "imported": bool(cells),
        "districts": sorted({c["district"] for c in cells}),
        "age_groups": sorted({c["age_group"] for c in cells}),
        "cells": cells,  # 内部台可见原始检出数
        "publications": get_publications(),
    }


@app.post("/api/preview")
async def preview(sel: Selection):
    cells = get_cells()
    if not cells:
        raise HTTPException(400, "尚未导入调查数据")
    matched, total = _match(cells, sel)
    candidate = {
        "districts": sel.districts,
        "age_groups": sel.age_groups,
        "total": total,
    }
    # 求解放入工作线程，求解期间公开页与其他请求不受影响。
    result = await asyncio.to_thread(evaluate, cells, get_publications(), candidate)
    return {"total": total, "cells_matched": len(matched), **result}


@app.post("/api/publish", status_code=201)
async def publish(sel: Selection):
    cells = get_cells()
    if not cells:
        raise HTTPException(400, "尚未导入调查数据")
    matched, total = _match(cells, sel)
    candidate = {
        "districts": sel.districts,
        "age_groups": sel.age_groups,
        "total": total,
    }

    def _evaluate_latest(publications: list[dict]) -> dict:
        return evaluate(cells, publications, candidate)

    # 跨进程原子发布：在 SQLite 写事务（BEGIN IMMEDIATE）内
    # 重读最新公开记录 → 重核 → 写入，任一失败整体回滚。
    # 两台服务实例各自安全、合并不安全时，后取得写锁者会看到先提交记录
    # 并因此被拒绝，因而只可能有一份被发布。
    try:
        result = await asyncio.to_thread(
            publish_guarded, sel.districts, sel.age_groups, total, _evaluate_latest
        )
    except DatabaseBusy:
        # 另一进程长时间持有写事务：本次未写入，可安全重试。
        raise HTTPException(503, "发布队列繁忙，未产生任何发布记录，请重试")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(500, "保存失败，未产生任何发布记录")

    if result["status"] != "published":
        evaluation = result["evaluation"]
        raise HTTPException(
            status_code=409,
            detail={
                "message": "按最新公开记录重核未通过：可能暴露小样本格值，已拒绝发布",
                "affected": evaluation["affected"],
            },
        )
    return result["publication"]


@app.get("/api/public")
def public_data():
    """公开数据：仅人数与获准汇总，绝不包含格内检出数。

    在单个只读事务中读取，且 WAL 允许读与写并发：
    其他进程正在确认发布时，本页仍可正常读取。
    """
    cells, pubs = get_public_snapshot()
    return {
        "imported": bool(cells),
        "cells": [
            {
                "district": c["district"],
                "age_group": c["age_group"],
                "population": c["population"],
            }
            for c in cells
        ],
        "publications": pubs,
    }
