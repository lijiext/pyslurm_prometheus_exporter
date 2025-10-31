# pyslurm-prometheus-exporter

基于 PySlurm 的 Prometheus Exporter SDK，可快速暴露 Slurm 作业、节点、分区等核心指标。

## 安装

```bash
pip install pyslurm-prometheus-exporter
```

（如需本地构建，可在项目目录执行 `python -m build` 后安装 `dist/` 下生成的 wheel。）

## 快速开始

```bash
pyslurm-exporter --port 9808 --addr 0.0.0.0
```

访问 `http://<host>:9808/metrics` 获取指标；若需单节点维度可追加 `--node-detail`。

若希望以库的方式集成，可在代码中调用：

```python
from slurm_prometheus_exporter import SlurmCollector, start_http_exporter

collector = SlurmCollector(include_node_detail=False)
start_http_exporter(port=9808, collector=collector)
```

更多示例与说明可参考仓库根目录 `docs/prometheus_exporter.md` 与 `examples/prometheus_exporter.py`。
