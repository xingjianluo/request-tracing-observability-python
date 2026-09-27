"""进程内可观测性状态单例。

独立成模块是为了打破 ``admin`` 与 ``middleware`` 之间的循环导入：
两边都依赖这里的注册表，而彼此不互相依赖。
"""
from __future__ import annotations

from .events import EventLog
from .metrics import MetricsRegistry

#: 全局指标注册表与事件日志（本地进程内，测试可直接 reset）。
metrics = MetricsRegistry()
event_log = EventLog()
