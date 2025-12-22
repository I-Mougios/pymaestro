# src/pymaestro/pymaestro.py
import json
from inspect import iscoroutinefunction
from pathlib import Path
from typing import Any, Callable, Optional

from .job_registry import JobRegistry
from .jobs import Job, JobPool, create_job
from .utils import DependsOn
from .utils.deserialize import deserialize
from .utils.serialize import serialize

__all__ = ["Maestro"]


class Maestro:
    """
    Singleton class responsible for managing the global job registry.

    Maestro ensures there is only one instance in the application and provides
    a central access point to add, retrieve, and organize jobs via the
    associated JobRegistry.

    Attributes:
        _registry (JobRegistry): The internal registry storing all jobs.
    """

    _instance = None

    def __new__(cls, registry: Optional[JobRegistry] = None):
        if not cls._instance:
            new_instance = super().__new__(cls)
            cls._instance = new_instance

        return cls._instance

    def __init__(self, registry: Optional[JobRegistry] = None):
        self._registry = registry or JobRegistry()

    @property
    def registry(self) -> JobRegistry:
        return self._registry

    def add(
        self,
        _executable: Optional[Callable[..., Any] | str] = None,
        *,
        job_type: str | None = None,
        name: str | None = None,
        parallel_group: str | None = None,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> Callable[..., Any]:
        """
        Register a job in the Maestro job pool.

        This method serves two purposes:

        1. **Simple decorator syntax**:
           ```python
           @maestro.add
           def my_task():
               ...
           ```
           - Registers the function as a `CallableJob` or `AsyncCallableJob` depending
             on whether it is synchronous or asynchronous.
           - Only supported without arguments, keyword arguments, or parallel groups.
           - Useful for lightweight job definitions.

        2. **Decorator factory syntax**:
           ```python
           @maestro.add(name="task_with_args", args=(42,), kwargs={"x": 10}, parallel_group="A")
           def my_task(a, x=0):
               ...
           ```
           - Provides full control over job registration (job type, name, args, kwargs, parallel group).
           - Recommended when jobs require parameters.

        3. **Direct invocation**:
           ```python
           maestro.add("./scripts/script_01.py", job_type="script", name="daily_etl", parallel_group="batch")
           maestro.add(".mypackage.mymodule.my_func", job_type="callable", name="callable as string")
           ```
           - Can be used to register a `ScriptJob`.
           - Can be used to register a CallableJob using the import path of the callable.
           - Not supported with decorator syntax (`@maestro.add`).

        Parameters
        ----------
        _executable : Callable, optional
            The function to register when using simple decorator syntax.
        job_type : str, optional
            The job type. Defaults to `"callable"` or `"async_callable"`
            when decorating functions, can be `"script"` when registering explicitly.
        name : str, optional
            A name for the job. If not provided, falls back to the function’s `__name__`.
        parallel_group : str, optional
            Group identifier for parallel execution control.
        args : tuple, optional
            Positional arguments to bind to the job.
        kwargs : dict, optional
            Keyword arguments to bind to the job.

        Returns
        -------
        Callable
            The original function when used as a decorator, or a decorator function
            when used as a factory.
        """

        def decorator(executable: Callable[..., Any] | str) -> Callable[..., Any] | str:
            nonlocal name, job_type

            # Require job_type when using a string
            if isinstance(executable, str) and job_type is None:
                raise ValueError(
                    "Parameter 'job_type' is required when registering a job by string path "
                    f"(got executable='{executable}')."
                )
            # Support for simple decorator syntax @maestro.add
            if job_type is None and callable(executable):
                job_type = "async_callable" if iscoroutinefunction(executable) else "callable"

            # Support for (1) the simple decorator syntax @maestro.add
            #             (2) convenient initialization of Job instances
            if name is None:
                try:
                    name = executable if isinstance(executable, str) else executable.__name__
                except AttributeError:
                    name = f"{executable.__class__.__name__}.__call__"

            job = create_job(
                job_type, name=name, executable=executable, parallel_group=parallel_group, args=args, kwargs=kwargs
            )
            self.registry.append(job)
            return executable

        return decorator(_executable) if _executable else decorator

    def swap(self, idx_or_name_i: int | str, idx_or_name_j: int | str) -> None:
        self.registry.swap(idx_or_name_i, idx_or_name_j)

    def clear(self) -> None:
        self.registry.clear()

    def execute(self) -> list[Any]:
        """Execute all the scheduled job"""
        results = []
        for job, priority in self.registry.grouped_jobs.items():
            self._resolve_dependencies(job)
            print(f"Executing job: \n  - Priority: {priority}\n  - Job name: '{job.name}'")  # noqa: T201
            if isinstance(job, JobPool):
                result = list(job.execute())
            else:
                result = job.execute()

            results.append(result)

        return results

    def _resolve_dependencies(self, job: Job) -> None:
        if hasattr(job, "args") or hasattr(job, "kwargs"):
            args = []
            for arg in job.args:
                if isinstance(arg, DependsOn):
                    idx = self.registry.index(arg.name)
                    args.append(self.registry[idx].result)
                else:
                    args.append(arg)

            job.args = tuple(args)

            for k, v in job.kwargs.items():
                if isinstance(v, DependsOn):
                    idx = self.registry.index(v.name)
                    job.kwargs[k] = self.registry[idx].result

    def serialize(self, path: str | Path | None = None) -> str:
        """
        Serialize the current state of JobRegistry
        """
        json_str = json.dumps(self.registry, default=serialize, indent=2)
        if path:
            path = Path(path)
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()

            path.write_text(json_str)

        return json_str

    def deserialize(self, path: str | Path | None) -> None:
        """
        Initialize the current state of JobRegistry from a JSON string
        """
        with open(str(path), "r") as f:
            registry = json.load(f, object_hook=deserialize)

        self._registry = registry
