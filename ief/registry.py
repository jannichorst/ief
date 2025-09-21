"""Task registry allowing capability-based lookup."""

from __future__ import annotations

import inspect
from typing import Callable, Iterable, MutableMapping

from .exceptions import RegistryError
from .tasks import Task

TaskFactory = Callable[[], Task]


class TaskRegistry:
    """Map string capabilities to task factories."""

    def __init__(self) -> None:
        self._factories: MutableMapping[str, TaskFactory] = {}

    def register(self, capability: str, factory: TaskFactory | type[Task]) -> None:
        if capability in self._factories:
            raise RegistryError(f"Capability {capability!r} already registered")
        if inspect.isclass(factory):
            if not issubclass(factory, Task):  # type: ignore[arg-type]
                raise RegistryError("Factory class must inherit from Task")
            self._factories[capability] = factory  # type: ignore[assignment]
        else:
            self._factories[capability] = factory  # type: ignore[assignment]

    def register_task(self, capability: str) -> Callable[[TaskFactory | type[Task]], TaskFactory | type[Task]]:
        def decorator(factory: TaskFactory | type[Task]) -> TaskFactory | type[Task]:
            self.register(capability, factory)
            return factory

        return decorator

    def create(self, capability: str) -> Task:
        try:
            factory = self._factories[capability]
        except KeyError as exc:
            raise RegistryError(f"Unknown capability {capability!r}") from exc
        if inspect.isclass(factory):
            task = factory()  # type: ignore[call-arg]
        else:
            task = factory()
        if not getattr(task, "id", None):
            task.id = capability
        return task

    def __contains__(self, capability: str) -> bool:
        return capability in self._factories

    def capabilities(self) -> Iterable[str]:
        return self._factories.keys()
