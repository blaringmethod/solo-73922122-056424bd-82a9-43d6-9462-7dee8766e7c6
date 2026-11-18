"""真正的跨进程竞态自测：两个独立服务进程共用同一个 SQLite 文件。

背景：进程内 asyncio.Lock 无法约束两个服务进程。本测试启动两个真实的
uvicorn 进程（不同端口、同一 DB_PATH），让它们在同一时刻确认两个
"各自安全、合并后暴露小样本格"的窗口，验证：

  A. 只可能有一份被发布（201），另一方得到 409 且带受影响格与可行范围；
  B. 最终库中确实只有一条记录，且与返回 201 的实例一致；
  C. 公开页在对方审查/写入期间仍可读取，且不含 detected；
  D. 合并安全的两个窗口并行确认则两份都可发布。

运行：python tests/cross_process_test.py
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable

SAMPLE = (
    "分区,年龄组,人数,检出数\n"
    "城东,青年,10,2\n"
    "城东,老年,10,8\n"
    "城西,青年,10,8\n"
    "城西,老年,10,8\n"
)

results = []


def check(name, cond, extra=""):
    results.append(bool(cond))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}")
    if not cond and extra:
        print("       " + str(extra).replace("\n", "\n       ")[:1200])


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(method, url, payload=None, timeout=60):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read() or b"{}")
        except Exception:
            body = {}
        return e.code, body


def wait_up(url, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            code, _ = http("GET", url + "/api/public", timeout=2)
            if code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.15)
    return False


def upload_sample(base):
    boundary = "----cdcselftest"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="s.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n" + SAMPLE + f"\r\n--{boundary}--\r\n"
    ).encode("utf-8")
    req = urllib.request.Request(
        base + "/api/import", data=body, method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def start_server(db_path, port, label, tmp):
    """启动独立 uvicorn 进程；用停止文件控制退出（沙箱可能禁止发信号）。"""
    stop = str(Path(tmp) / f"stop_{port}.flag")
    env = dict(os.environ, DB_PATH=db_path, INSTANCE_LABEL=label, STOP_FILE=stop)
    log = open(Path(tmp) / f"uvicorn_{port}.log", "w")
    proc = subprocess.Popen(
        [PY, str(ROOT / "tests" / "_serve.py"), str(port)],
        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    return proc, stop


def stop_server(proc, stop):
    Path(stop).write_text("stop")
    try:
        proc.wait(timeout=20)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def main():
    tmp = tempfile.mkdtemp(prefix="cdc_xproc_")
    procs = []          # [(proc, stop_file)]
    try:
        ports = [free_port(), free_port()]
        db = str(Path(tmp) / "shared.db")
        for i, port in enumerate(ports):
            procs.append(start_server(db, port, f"A{i+1}", tmp))
        bases = [f"http://127.0.0.1:{p}" for p in ports]
        check("两个独立服务进程均已启动",
              all(wait_up(b) for b in bases), "请查看临时目录日志")

        check("实例 A 导入示例数据", upload_sample(bases[0]) == 201)
        # 实例 B 能看到同一库中的导入数据（跨进程共享）
        code, j = http("GET", bases[1] + "/api/admin/state")
        check("实例 B 读到实例 A 导入的数据（共享库）",
              code == 200 and j.get("imported") and len(j["cells"]) == 4, j)

        # 并发导入：另一实例再导入不得覆盖，得到 409 且库仍是原 4 格
        import concurrent.futures as cf
        with cf.ThreadPoolExecutor(max_workers=1) as ex:
            dup = ex.submit(upload_sample, bases[1]).result(timeout=60)
        check("另一实例重复导入被拒（409，导入后不可更换）", dup == 409, dup)
        _c, st = http("GET", bases[0] + "/api/admin/state")
        check("重复导入后格值仍为 4 格", len(st["cells"]) == 4, st)

        # 两个窗口：单独预览都安全，合并后「城东/青年」被钉在 [0,4]
        w1 = {"districts": ["城东"], "age_groups": ["青年", "老年"]}
        w2 = {"districts": ["城东", "城西"], "age_groups": ["老年"]}
        for base in bases:
            for w in (w1, w2):
                c, j = http("POST", base + "/api/preview", w)
                assert c == 200 and j["safe"], (c, j)
        check("两个窗口在各自实例上单独预览均安全", True)

        # ---- 并发确认：恰好一份成功 ----
        import concurrent.futures as cf
        with cf.ThreadPoolExecutor(max_workers=2) as ex:
            futs = [ex.submit(http, "POST", b + "/api/publish", w)
                    for b, w in zip(bases, (w1, w2))]
            outcomes = [f.result(timeout=120) for f in futs]
        codes = sorted(c for c, _ in outcomes)
        check("并发确认结果恰为一份 201、一份 409", codes == [201, 409], outcomes)

        rejected = next((b for c, b in outcomes if c == 409), {})
        aff = rejected.get("detail", {}).get("affected") or []
        check("被拒方返回受影响格 城东/青年 与可行范围 [0,4]",
              len(aff) == 1
              and aff[0]["district"] == "城东" and aff[0]["age_group"] == "青年"
              and aff[0]["min"] == 0 and aff[0]["max"] == 4
              and aff[0]["zero_feasible"] and not aff[0]["ge5_feasible"],
              rejected)

        # ---- 公开页在两台实例上读到一致且仅一条记录 ----
        pubs = [http("GET", b + "/api/public")[1] for b in bases]
        check("两台实例公开页均只有 1 条公开记录",
              all(len(p["publications"]) == 1 for p in pubs),
              [p["publications"] for p in pubs])
        check("两实例公开记录一致（同一库）",
              pubs[0]["publications"] == pubs[1]["publications"])
        check("公开页不含 detected 字段",
              all("detected" not in c for p in pubs for c in p["cells"]))

        # ---- 读取期间不被写阻塞：审查进行时公开页仍可读 ----
        # 触发一次重核（含求解），同时高频读取公开页。
        stop = time.time() + 3
        reads, errors = 0, 0
        import threading
        def hammer():
            nonlocal reads, errors
            while time.time() < stop:
                try:
                    c, j = http("GET", bases[0] + "/api/public", timeout=5)
                    if c == 200:
                        reads += 1
                    else:
                        errors += 1
                except Exception:
                    errors += 1
        t = threading.Thread(target=hammer)
        t.start()
        for _ in range(10):
            http("POST", bases[1] + "/api/publish", w2)
            http("POST", bases[0] + "/api/preview", w1)
        t.join()
        check(f"审查/写入期间公开页持续可读（{reads} 次成功，{errors} 次失败）",
              reads > 0 and errors == 0, f"reads={reads} errors={errors}")

        # ---- 合并安全的两个窗口并行确认：两份都应成功 ----
        # 新库重来，避免之前记录影响。
        for proc, stop in procs:
            stop_server(proc, stop)
        procs.clear()
        shutil.rmtree(tmp, ignore_errors=True)
        tmp = tempfile.mkdtemp(prefix="cdc_xproc2_")
        db2 = str(Path(tmp) / "shared.db")
        ports2 = [free_port(), free_port()]
        for i, port in enumerate(ports2):
            procs.append(start_server(db2, port, f"B{i+1}", tmp))
        bases2 = [f"http://127.0.0.1:{p}" for p in ports2]
        assert all(wait_up(b) for b in bases2)
        assert upload_sample(bases2[0]) == 201

        # 「城东/青年+老年」=10 与「城东+城西/青年」=10 合并后仍安全（示例数据中
        # 城东/青年=2 仍可 0 或 ≥5：两式给 城东老年+城西青年=8 等，未钉死）。
        s1 = {"districts": ["城东"], "age_groups": ["青年", "老年"]}
        s2 = {"districts": ["城西"], "age_groups": ["青年", "老年"]}
        safe_previews = [http("POST", b + "/api/preview", w)
                         for b, w in zip(bases2, (s1, s2))]
        check("两个合并安全的窗口单独预览均安全",
              all(c == 200 and j["safe"] for c, j in safe_previews), safe_previews)
        with cf.ThreadPoolExecutor(max_workers=2) as ex:
            futs = [ex.submit(http, "POST", b + "/api/publish", w)
                    for b, w in zip(bases2, (s1, s2))]
            outs = [f.result(timeout=120) for f in futs]
        check("合并安全时并行确认两份都可发布（均 201）",
              [c for c, _ in outs] == [201, 201], outs)
        n = http("GET", bases2[0] + "/api/public")[1]["publications"]
        check("最终库中有 2 条公开记录", len(n) == 2, n)
    finally:
        for proc, stop in procs:
            stop_server(proc, stop)
        shutil.rmtree(tmp, ignore_errors=True)


main()
print(f"\n{sum(results)}/{len(results)} 通过")
sys.exit(0 if all(results) else 1)
