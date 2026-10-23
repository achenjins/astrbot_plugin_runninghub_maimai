"""Compatibility import; queue coordination is maintained in the upstream project."""
from .upstream.task_queue import TaskLimiter

__all__ = ["TaskLimiter"]
