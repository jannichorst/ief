"""Core package for the lean information extraction pipeline prototype.

This package currently provides basic task specifications and
utilities for working with metrics that follow the standardized naming
from the project specification.  Modules intentionally stay lightweight
so the rest of the repository can build on top of them without pulling
in heavy dependencies.
"""

from .core_tasks import CORE_TASKS, CoreTask, get_core_task_ids
from . import utils

__all__ = ["CORE_TASKS", "CoreTask", "get_core_task_ids", "utils"]
