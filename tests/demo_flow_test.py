"""端到端演示流程自测：模拟浏览器的"并行确认"按钮，走两台真实服务进程。

覆盖从用户视角的完整链路：
- 实例 A 导入示例数据（跨进程共享）
- 页面「填入合并不安全的窗口」→ Promise.all 同时向 A 与 B 确认
  → 恰一份 201、一份 409，且 409 含受影响格与可行范围
- 公开页在两实例上一致且不含 detected
- 换成"合并安全的窗口" → 并行确认两份都成功
- 跨源请求（带 Origin）能被 CORS 接受（页面从 :8000 调 :8001）

运行：python tests/demo_flow_test.py
"""

import concurrent.futures as cf
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


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(method, url, payload=None, origin=None, timeout=60):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    if origin:
        headers["Origin"] = origin
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}"), dict(r.headers)
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read() or b"{}")
        except Exception:
            body = {}
        return e.code, body, dict(e.headers)


def wait_up(url, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if http("GET", url + "/api/public", timeout=2)[0] == 200:
                return True
        except Exception:
            pass
        time.sleep(0.15)
    return False


def upload_sample(base):
    boundary = "----cdcdemo"
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


def start(db, port, label, peer, tmp):
    stop = str(Path(tmp) / f"stop_{port}.flag")
    env = dict(os.environ, DB_PATH=db, INSTANCE_LABEL=label,
               PEER_URL=peer, STOP_FILE=stop)
    log = open(Path(tmp) / f"uvicorn_{port}.log", "w")
    p = subprocess.Popen([PY, str(ROOT / "tests" / "_serve.py"), str(port)],
                         cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    return p, stop


def stop(p, stop):
    Path(stop).write_text("x")
    try:
        p.wait(timeout=20)
    except Exception:
        try:
            p.kill()
        except Exception:
            pass


def main():
    tmp = tempfile.mkdtemp(prefix="cdc_demo_")
    procs = []
    try:
        pa, pb = free_port(), free_port()
        ua, ub = f"http://127.0.0.1:{pa}", f"http://127.0.0.1:{pb}"
        db = str(Path(tmp) / "shared.db")
        procs.append(start(db, pa, "实例 A", ub + "/", tmp))
        procs.append(start(db, pb, "实例 B", ua + "/", tmp))
        check("两台实例启动", wait_up(ua) and wait_up(ub))
        check("实例 A 导入示例数据", upload_sample(ua) == 201)

        # 实例名与对端地址被正确暴露给页面
        _, sa, _ = http("GET", ua + "/api/admin/state")
        _, sb, _ = http("GET", ub + "/api/admin/state")
        check("两实例分别报告自己的实例名",
              sa["instance"] == "实例 A" and sb["instance"] == "实例 B", (sa, sb))
        check("两实例的 PEER_URL 指向对方",
              sa["peer_url"].rstrip("/") == ub and sb["peer_url"].rstrip("/") == ua)

        # CORS：页面从 A(:pa) 调 B(:pb) 的预检要被接受
        import http.client as httpclient
        from urllib.parse import urlparse
        pu = urlparse(ub)
        conn = httpclient.HTTPConnection(pu.hostname, pu.port, timeout=10)
        conn.request("OPTIONS", "/api/publish", headers={
            "Origin": ua,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        })
        presp = conn.getresponse()
        allow = presp.getheader("access-control-allow-origin")
        conn.close()
        check("跨源预检被接受（页面可调对端）",
              presp.status == 200 and allow in ("*", ua), (presp.status, allow))

        # 页面「合并不安全」按钮对应的两个窗口，并行确认
        wA = {"districts": ["城东"], "age_groups": ["青年", "老年"]}
        wB = {"districts": ["城东", "城西"], "age_groups": ["老年"]}
        for base, w in ((ua, wA), (ub, wB)):
            c, prev, _ = http("POST", base + "/api/preview", w)
            assert c == 200 and prev["safe"], (c, prev)

        def confirm(base, w, origin=None):
            return http("POST", base + "/api/publish", w, origin=origin)

        with cf.ThreadPoolExecutor(max_workers=2) as ex:
            fa = ex.submit(confirm, ua, wA, ua)     # 本实例（带 Origin，模拟页面）
            fb = ex.submit(confirm, ub, wB, ua)     # 对端（页面跨源调用 B）
            ra, rb = fa.result(120), fb.result(120)
        statuses = sorted([ra[0], rb[0]])
        check("合并不安全时恰一份 201、一份 409", statuses == [201, 409],
              (ra[:2], rb[:2]))

        rej = ra if ra[0] == 409 else rb
        aff = (rej[1].get("detail") or {}).get("affected") or []
        check("被拒方给出 城东/青年 与可行范围 [0,4]",
              len(aff) == 1 and aff[0]["district"] == "城东"
              and aff[0]["age_group"] == "青年"
              and aff[0]["min"] == 0 and aff[0]["max"] == 4, rej[:2])

        _, pa_pub, _ = http("GET", ua + "/api/public")
        _, pb_pub, _ = http("GET", ub + "/api/public")
        check("两实例公开页记录一致且仅一条",
              pa_pub["publications"] == pb_pub["publications"]
              and len(pa_pub["publications"]) == 1,
              (pa_pub["publications"], pb_pub["publications"]))
        check("公开页无 detected 字段",
              all("detected" not in c for p in (pa_pub, pb_pub) for c in p["cells"]))

        # ---- 同一库上，换用"合并安全的窗口"：两份都应发布成功 ----
        # 直接用新库更清晰（已有记录会影响判断），重启两台实例指向新库。
        for p, s in procs:
            stop(p, s)
        procs.clear()
        shutil.rmtree(tmp, ignore_errors=True)
        tmp = tempfile.mkdtemp(prefix="cdc_demo_safe_")
        pa2, pb2 = free_port(), free_port()
        ua2, ub2 = f"http://127.0.0.1:{pa2}", f"http://127.0.0.1:{pb2}"
        db2 = str(Path(tmp) / "shared.db")
        procs.append(start(db2, pa2, "实例 A", ub2 + "/", tmp))
        procs.append(start(db2, pb2, "实例 B", ua2 + "/", tmp))
        assert wait_up(ua2) and wait_up(ub2)
        assert upload_sample(ua2) == 201
        sA = {"districts": ["城东"], "age_groups": ["青年", "老年"]}
        sB = {"districts": ["城西"], "age_groups": ["青年", "老年"]}
        for base, w in ((ua2, sA), (ub2, sB)):
            c, prev, _ = http("POST", base + "/api/preview", w)
            assert c == 200 and prev["safe"], (c, prev)
        with cf.ThreadPoolExecutor(max_workers=2) as ex:
            fa = ex.submit(http, "POST", ua2 + "/api/publish", sA, ua2)
            fb = ex.submit(http, "POST", ub2 + "/api/publish", sB, ua2)
            ra, rb = fa.result(120), fb.result(120)
        check("合并安全时并行确认两份都成功（均 201）",
              ra[0] == 201 and rb[0] == 201, (ra[:2], rb[:2]))
        n = http("GET", ua2 + "/api/public")[1]["publications"]
        check("最终库中 2 条公开记录", len(n) == 2, n)
    finally:
        for p, s in procs:
            stop(p, s)
        shutil.rmtree(tmp, ignore_errors=True)


main()
print(f"\n{sum(results)}/{len(results)} 通过")
sys.exit(0 if all(results) else 1)
