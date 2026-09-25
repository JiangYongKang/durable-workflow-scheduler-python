"""Domain errors."""
from __future__ import annotations


class SchedulerError(Exception):
    """Base error for the scheduler package."""


class ValidationError(SchedulerError):
    """Workflow definition is invalid (e.g. cyclic dependency)."""


class TaskError(SchedulerError):
    """A task raised a business error."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class RetryableTaskError(TaskError):
    def __init__(self, message: str = "retryable task error") -> None:
        super().__init__(message, retryable=True)


class NonRetryableTaskError(TaskError):
    def __init__(self, message: str = "non-retryable task error") -> None:
        super().__init__(message, retryable=False)


class InstanceTimedOut(SchedulerError):
    """Raised internally when an instance/node deadline is exceeded."""


class WorkflowNotFound(SchedulerError):
    """Workflow name is not registered."""
