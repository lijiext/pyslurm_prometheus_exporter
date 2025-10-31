# -*- coding: utf-8 -*-
"""
提供启动Exporter的命令行工具。
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from typing import Optional

from .collector import SlurmCollector, start_http_exporter


def build_parser() -> argparse.ArgumentParser:
    """创建命令行解析器。"""
    parser = argparse.ArgumentParser(
        description="启动基于PySlurm的Prometheus Exporter，用于采集Slurm集群指标。",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9808,
        help="监听端口，默认9808",
    )
    parser.add_argument(
        "--addr",
        default="0.0.0.0",
        help="监听地址，默认0.0.0.0",
    )
    parser.add_argument(
        "--node-detail",
        action="store_true",
        help="启用单节点细粒度指标（可能导致标签基数较高）。",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别，默认INFO",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """CLI 入口。"""
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.info("初始化SlurmCollector，include_node_detail=%s", args.node_detail)

    collector = SlurmCollector(include_node_detail=args.node_detail)
    start_http_exporter(port=args.port, addr=args.addr, collector=collector)

    stop_event = threading.Event()

    def _signal_handler(signum: int, _frame: object) -> None:
        logging.info("收到信号%s，准备退出", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logging.info(
        "Exporter已启动，访问 http://%s:%d/metrics 查看指标",
        args.addr,
        args.port,
    )

    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=1.0)
    finally:
        logging.info("Exporter已退出")

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
