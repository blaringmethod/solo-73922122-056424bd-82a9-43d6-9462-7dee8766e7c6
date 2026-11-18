"""测试辅助：启动一个 uvicorn 服务，并在出现停止文件时自行退出。

沙箱环境可能禁止父进程向子进程发送信号，因此测试用 ``STOP_FILE``
环境变量指定的文件来优雅停止服务（删除该文件不影响运行）。

用法（仅测试）：STOP_FILE=/tmp/x.stop python tests/_serve.py <port>
"""

import os
import sys
import threading
import time
from pathlib import Path

import uvicorn

# 以脚本方式运行时 sys.path[0] 是 tests/，需显式加入项目根目录。
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> None:
    port = int(sys.argv[1])
    stop_file = os.environ.get("STOP_FILE")
    server = uvicorn.Server(
        uvicorn.Config("app.main:app", host="127.0.0.1", port=port, log_level="warning")
    )

    def watchdog() -> None:
        while not os.path.exists(stop_file):
            time.sleep(0.1)
        server.should_exit = True

    if stop_file:
        threading.Thread(target=watchdog, daemon=True).start()
    server.run()


main()
