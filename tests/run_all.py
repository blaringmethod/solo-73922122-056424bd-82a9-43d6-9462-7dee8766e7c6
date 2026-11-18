"""一次运行全部自测：单进程功能、跨进程竞态、端到端演示流程。

运行：python tests/run_all.py
各子测试也可单独运行；任一失败则整体退出码非 0。
"""

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SUITES = ["selftest.py", "cross_process_test.py", "demo_flow_test.py"]

failed = []
for name in SUITES:
    print(f"\n===== {name} =====", flush=True)
    r = subprocess.run([sys.executable, str(HERE / name)], cwd=HERE.parent)
    if r.returncode != 0:
        failed.append(name)

print("\n===== 汇总 =====")
if failed:
    print("失败：" + "、".join(failed))
    sys.exit(1)
print("全部通过")
