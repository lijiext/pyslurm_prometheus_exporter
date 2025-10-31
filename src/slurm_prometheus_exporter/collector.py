# -*- coding: utf-8 -*-
"""
提供独立的Slurm Prometheus采集器实现，便于打包分发。
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple

import pyslurm

try:
    from prometheus_client.core import GaugeMetricFamily, InfoMetricFamily
except ImportError:  # pragma: no cover - 运行时依赖缺失时提示用户
    GaugeMetricFamily = None  # type: ignore[assignment]
    InfoMetricFamily = None  # type: ignore[assignment]


FetchFn = Callable[[], Mapping[Any, Any]]


class SlurmCollector:
    """
    SlurmCollector实现prometheus_client的Collector接口，用于统一采集作业、节点、
    分区等指标，并聚合为Prometheus可读的MetricFamily实例。
    """

    def __init__(
        self,
        job_fetcher: Optional[FetchFn] = None,
        node_fetcher: Optional[FetchFn] = None,
        partition_fetcher: Optional[FetchFn] = None,
        clock: Callable[[], float] = time.time,
        logger: Optional[logging.Logger] = None,
        include_node_detail: bool = False,
    ) -> None:
        """
        Args:
            job_fetcher: 返回作业信息字典的可调用对象，默认使用pyslurm.job().get。
            node_fetcher: 返回节点信息字典的可调用对象，默认使用pyslurm.node().get。
            partition_fetcher: 返回分区信息字典的可调用对象，默认使用pyslurm.partition().get。
            clock: 提供当前时间戳的函数，便于注入测试时间源。
            logger: 日志记录器，默认使用模块级logger。
            include_node_detail: 是否输出单节点细分指标，默认关闭以控制标签基数。
        """
        self._ensure_prometheus_ready()

        self._job_fetcher = job_fetcher or self._default_job_fetcher
        self._node_fetcher = node_fetcher or self._default_node_fetcher
        self._partition_fetcher = partition_fetcher or self._default_partition_fetcher
        self._clock = clock
        self._logger = logger or logging.getLogger(__name__)
        self._include_node_detail = include_node_detail
        self._last_error: Optional[str] = None

    @staticmethod
    def _ensure_prometheus_ready() -> None:
        """在初始化阶段检查prometheus_client是否可用。"""
        if GaugeMetricFamily is None:
            raise RuntimeError(
                "未检测到prometheus-client库，请先执行 `pip install prometheus-client` 后再使用SlurmCollector。",
            )

    @staticmethod
    def _default_job_fetcher() -> Mapping[Any, Any]:
        """默认的作业采集方法。"""
        return pyslurm.job().get() or {}

    @staticmethod
    def _default_node_fetcher() -> Mapping[Any, Any]:
        """默认的节点采集方法。"""
        return pyslurm.node().get() or {}

    @staticmethod
    def _default_partition_fetcher() -> Mapping[Any, Any]:
        """默认的分区采集方法。"""
        return pyslurm.partition().get() or {}

    def collect(self) -> Iterable[Any]:
        """Prometheus Collector 接口实现：采集Slurm状态并生成指标。"""
        scrape_time = self._clock()
        scrape_success = 1.0
        last_error: Optional[str] = None

        job_state_counter: Counter[str] = Counter()
        job_cpu_counter: Counter[str] = Counter()
        node_state_counter: Counter[str] = Counter()
        node_cpu_capacity = 0
        node_cpu_allocated = 0
        partition_capacity: Dict[str, Tuple[int, int]] = {}
        node_details: Sequence[Tuple[str, int, int]] = ()

        try:
            jobs = self._job_fetcher()
            self._aggregate_jobs(jobs, job_state_counter, job_cpu_counter)
        except Exception as exc:  # pragma: no cover - 依赖环境不可控
            scrape_success = 0.0
            last_error = f"job_fetch_failed:{exc}"
            self._logger.exception("采集作业信息失败", exc_info=exc)

        try:
            nodes = self._node_fetcher()
            (
                node_state_counter,
                node_cpu_capacity,
                node_cpu_allocated,
                node_details,
            ) = self._aggregate_nodes(nodes)
        except Exception as exc:  # pragma: no cover - 依赖环境不可控
            scrape_success = 0.0
            msg = f"node_fetch_failed:{exc}"
            last_error = msg if last_error is None else f"{last_error};{msg}"
            self._logger.exception("采集节点信息失败", exc_info=exc)

        try:
            partitions = self._partition_fetcher()
            partition_capacity = self._aggregate_partitions(partitions)
        except Exception as exc:  # pragma: no cover - 依赖环境不可控
            scrape_success = 0.0
            msg = f"partition_fetch_failed:{exc}"
            last_error = msg if last_error is None else f"{last_error};{msg}"
            self._logger.exception("采集分区信息失败", exc_info=exc)

        self._last_error = last_error

        yield from self._emit_job_metrics(job_state_counter, job_cpu_counter)
        yield from self._emit_node_metrics(
            node_state_counter,
            node_cpu_capacity,
            node_cpu_allocated,
            node_details,
        )
        yield from self._emit_partition_metrics(partition_capacity)
        yield from self._emit_scrape_status(scrape_success, scrape_time, last_error)

    def _emit_job_metrics(
        self,
        job_state_counter: Counter[str],
        job_cpu_counter: Counter[str],
    ) -> Iterable[Any]:
        """输出与作业相关的Metric。"""
        job_gauge = GaugeMetricFamily(
            "slurm_job_state_total",
            "按状态聚合的Slurm作业数量",
            labels=["state"],
        )
        for state, count in sorted(job_state_counter.items()):
            job_gauge.add_metric([state], float(count))
        yield job_gauge

        cpu_gauge = GaugeMetricFamily(
            "slurm_job_cpus_total",
            "按作业状态聚合的CPU核数需求量",
            labels=["state"],
        )
        for state, cpu_count in sorted(job_cpu_counter.items()):
            cpu_gauge.add_metric([state], float(cpu_count))
        yield cpu_gauge

    def _emit_node_metrics(
        self,
        node_state_counter: Counter[str],
        node_cpu_capacity: int,
        node_cpu_allocated: int,
        node_details: Sequence[Tuple[str, int, int]],
    ) -> Iterable[Any]:
        """输出节点与集群相关的Metric。"""
        node_state_gauge = GaugeMetricFamily(
            "slurm_node_state_total",
            "按状态聚合的节点数量",
            labels=["state"],
        )
        for state, count in sorted(node_state_counter.items()):
            node_state_gauge.add_metric([state], float(count))
        yield node_state_gauge

        cluster_cpu_capacity_gauge = GaugeMetricFamily(
            "slurm_cluster_cpus_total",
            "集群CPU总量",
        )
        cluster_cpu_capacity_gauge.add_metric([], float(node_cpu_capacity))
        yield cluster_cpu_capacity_gauge

        cluster_cpu_allocated_gauge = GaugeMetricFamily(
            "slurm_cluster_cpus_allocated",
            "当前已分配的CPU核数",
        )
        cluster_cpu_allocated_gauge.add_metric([], float(node_cpu_allocated))
        yield cluster_cpu_allocated_gauge

        if self._include_node_detail and node_details:
            node_detail_gauge = GaugeMetricFamily(
                "slurm_node_cpu_usage",
                "单节点CPU容量与分配情况",
                labels=["node", "type"],
            )
            for node_name, total_cpus, alloc_cpus in node_details:
                node_detail_gauge.add_metric([node_name, "capacity"], float(total_cpus))
                node_detail_gauge.add_metric([node_name, "allocated"], float(alloc_cpus))
            yield node_detail_gauge

    def _emit_partition_metrics(
        self,
        partition_capacity: Dict[str, Tuple[int, int]],
    ) -> Iterable[Any]:
        """输出分区相关Metric。"""
        partition_gauge = GaugeMetricFamily(
            "slurm_partition_nodes_total",
            "每个分区的节点容量与上线数量",
            labels=["partition", "type"],
        )
        for part, (total_nodes, total_cpus) in sorted(partition_capacity.items()):
            partition_gauge.add_metric([part, "nodes"], float(total_nodes))
            partition_gauge.add_metric([part, "cpus"], float(total_cpus))
        yield partition_gauge

    def _emit_scrape_status(
        self,
        scrape_success: float,
        scrape_time: float,
        last_error: Optional[str],
    ) -> Iterable[Any]:
        """输出采集状态信息。"""
        scrape_success_gauge = GaugeMetricFamily(
            "slurm_exporter_last_scrape_success",
            "上一次拉取是否成功（1成功，0失败）",
        )
        scrape_success_gauge.add_metric([], scrape_success)
        yield scrape_success_gauge

        scrape_timestamp_gauge = GaugeMetricFamily(
            "slurm_exporter_last_scrape_timestamp",
            "最后一次成功采集的时间戳",
        )
        scrape_timestamp_gauge.add_metric([], float(scrape_time))
        yield scrape_timestamp_gauge

        if InfoMetricFamily is not None:
            info_metric = InfoMetricFamily(
                "slurm_exporter_runtime",
                "采集器运行时信息",
            )
            info_metric.add_metric(
                (),
                {
                    "error": (last_error or "")[:256],
                    "include_node_detail": str(self._include_node_detail).lower(),
                },
            )
            yield info_metric

    def _aggregate_jobs(
        self,
        jobs: Mapping[Any, Any],
        job_state_counter: Counter[str],
        job_cpu_counter: Counter[str],
    ) -> None:
        """统计作业状态与CPU请求量。"""
        for job in jobs.values():
            state = str(job.get("job_state", "UNKNOWN") or "UNKNOWN")
            norm_state = state.split("+", 1)[0]
            job_state_counter[norm_state] += 1

            cpu_count = self._extract_job_cpu(job)
            job_cpu_counter[norm_state] += cpu_count

    @staticmethod
    def _extract_job_cpu(job: Mapping[str, Any]) -> int:
        """尝试从作业信息中抽取CPU需求量。"""
        for key in ("num_cpus", "cpus_per_task", "cpus"):
            value = job.get(key)
            if isinstance(value, int):
                return max(value, 0)
            if isinstance(value, str) and value.isdigit():
                return int(value)
        return 0

    def _aggregate_nodes(
        self,
        nodes: Mapping[Any, Any],
    ) -> Tuple[Counter[str], int, int, Sequence[Tuple[str, int, int]]]:
        """统计节点状态以及CPU容量。"""
        state_counter: Counter[str] = Counter()
        total_cpu = 0
        alloc_cpu = 0
        node_detail: list[Tuple[str, int, int]] = []

        for name, node in nodes.items():
            state_raw = str(node.get("state", "UNKNOWN") or "UNKNOWN")
            state = state_raw.split("+", 1)[0]
            state_counter[state] += 1

            node_cpus = self._safe_int(node.get("cpus"))
            node_alloc = self._safe_int(node.get("alloc_cpus"))
            total_cpu += node_cpus
            alloc_cpu += min(node_alloc, node_cpus)

            if self._include_node_detail:
                node_detail.append((str(name), node_cpus, min(node_alloc, node_cpus)))

        return state_counter, total_cpu, alloc_cpu, node_detail

    def _aggregate_partitions(
        self,
        partitions: Mapping[Any, Any],
    ) -> Dict[str, Tuple[int, int]]:
        """统计分区中的节点与CPU容量。"""
        capacity: Dict[str, Tuple[int, int]] = {}

        for name, partition in partitions.items():
            total_nodes = self._safe_int(partition.get("total_nodes"))
            total_cpus = self._safe_int(partition.get("total_cpus"))
            capacity[str(name)] = (total_nodes, total_cpus)

        return capacity

    @staticmethod
    def _safe_int(value: Any) -> int:
        """确保数值转换，无法解析时返回0。"""
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            digits = "".join(ch for ch in value if ch.isdigit())
            if digits:
                return int(digits)
        return 0

    @property
    def last_error(self) -> Optional[str]:
        """返回最近一次采集的错误信息。"""
        return self._last_error


def register_collector(
    registry: Optional[Any] = None,
    collector: Optional[SlurmCollector] = None,
) -> Any:
    """
    将SlurmCollector注册到指定的CollectorRegistry中。
    """
    from prometheus_client import REGISTRY

    target_registry = registry or REGISTRY
    target_collector = collector or SlurmCollector()
    target_registry.register(target_collector)
    return target_registry


def start_http_exporter(
    port: int = 9808,
    addr: str = "0.0.0.0",
    registry: Optional[Any] = None,
    collector: Optional[SlurmCollector] = None,
) -> None:
    """
    启动一个简单的HTTP端点用于暴露Slurm指标。
    """
    from prometheus_client import CollectorRegistry, start_http_server

    target_registry = registry or CollectorRegistry()
    target_collector = collector or SlurmCollector()
    target_registry.register(target_collector)
    start_http_server(port, addr=addr, registry=target_registry)
