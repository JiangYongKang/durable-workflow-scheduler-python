"""错误类型：区分可重试错误与不可重试（致命）错误。"""
from __future__ import annotations


class WorkflowError(Exception):
    """引擎内部错误基类。"""


class CycleDetectedError(WorkflowError):
    """提交阶段检测到环形依赖。"""


class UnknownTaskError(WorkflowError):
    """依赖引用了不存在的任务。"""


class RetryableTaskError(Exception):
    """任务执行中的可重试错误：将按退避策略重试，直至次数上限。"""

    kind = "retryable"


class FatalTaskError(Exception):
    """任务执行中的不可重试错误：立即置为 FAILED，不再重试。"""

    kind = "fatal"
