# -*- coding: utf-8 -*-
"""
slurm_prometheus_exporter: 基于PySlurm封装的Prometheus采集SDK。
"""

from .collector import SlurmCollector, register_collector, start_http_exporter
from .cli import main

__all__ = [
    "SlurmCollector",
    "register_collector",
    "start_http_exporter",
    "main",
]
