# src/pymaestro/jobs.py
"""
pymaestro.jobs
============

This module defines the core job abstraction layer for Maestro.

It includes:
- Base and derived Job classes (callable, async, and script jobs)
- A factory function (`create_job`) that implements the Factory design pattern
- A `serialize` singledispatch function to convert jobs to JSON-serializable structures
"""

import asyncio
import concurrent.futures
import importlib
import importlib.util
import runpy
from abc import ABC, abstractmethod
from inspect import iscoroutinefunction
from pathlib import Path
from typing import Any, Callable, Iterator

from .utils.dispatcher import Dispatcher
from .utils.wrappers import inject_dependencies, is_completed

__all__ = [
    "Job",
    "JobPool",
    "CallableJob",
    "AsyncCallableJob",
    "ScriptJob",
    "create_job",
    "SUPPORTED_JOB_TYPES",
]

# ---------------------------------------------------------------------------
#  Base Job Abstractions
# ---------------------------------------------------------------------------

SUPPORTED_JOB_TYPES = {"callable", "async_callable", "script"}


class Job(ABC):
    def __init__(self, name: str, executable: str | Callable[..., Any], parallel_group: str | None = None) -> None:
        self.name = name
        self.executable = executable
        self.parallel_group = parallel_group

        self.is_completed: bool | None = None
        self._result: Any = None

    def __init_subclass__(cls):
        execute = cls.execute
        cls.execute = is_completed(inject_dependencies(execute))

    @abstractmethod
    def execute(self) -> Any:
        pass

    @property
    def result(self) -> Any:
        if self.is_completed:
            return self._result

        return self.execute()

    @result.setter
    def result(self, value: Any) -> None:
        self._result = value

    def __getstate__(self) -> dict[str, Any]:
        return {"is_completed": self.is_completed, "result": self.result}

    def __reduce__(self):
        creator = self.__class__
        args = (self.name, self.executable, self.parallel_group)
        state = self.__getstate__()
        return creator, args, state

    def __setstate__(self, state):
        for key, value in state.items():
            setattr(self, key, value)

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other) -> bool:
        if isinstance(other, JobPool):
            return False
        elif isinstance(other, Job):
            return type(self) is type(other) and self.name == other.name

        return NotImplemented


class JobPool(Job):
    def __init__(self, *jobs: Job) -> None:  # noqa
        for job in jobs:
            if not isinstance(job, Job):  # Ensure that elements passed are of type Job
                raise TypeError("Elements passed to JobPool must be instances of 'Job'.")
            if isinstance(job, JobPool):  # Ensure no nested JobPools are passed
                raise ValueError("Nested JobPools are not allowed")

        # Collect all unique parallel group names from the jobs
        parallel_groups = {job.parallel_group for job in jobs}

        # Ensure all jobs belong to exactly one parallel group
        if len(parallel_groups) != 1 or None in parallel_groups:
            raise ValueError(
                f"All jobs in a JobPool must have and share the same parallel group. "
                f"Found parallel groups: {parallel_groups or 'None'}"
            )

        self.jobs = jobs
        self.parallel_group = parallel_groups.pop()

    @property
    def name(self) -> str:
        return self.parallel_group

    @staticmethod
    def execute_job(job: Job) -> Any:
        return job.execute()

    def execute(self, max_workers: int | None = None, mode: str = "as_submitted") -> Iterator[Any]:
        if mode not in ("as_submitted", "as_completed"):
            raise ValueError("'mode' must be 'as_submitted' or 'as_completed'")

        if max_workers is None:
            import os

            max_workers = os.cpu_count()

        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            if mode == "as_completed":
                futures = [executor.submit(self.execute_job, job) for job in self]
                for res in concurrent.futures.as_completed(futures):
                    yield res.result()
            else:
                results = executor.map(JobPool.execute_job, self)
                for result in results:
                    yield result

    def __reduce__(self):
        raise TypeError("JobPool instances cannot be pickled. Nested pools are forbidden.")

    def __len__(self) -> int:
        return len(self.jobs)

    def __getitem__(self, idx: int) -> Job:
        return self.jobs[idx]

    def __iter__(self) -> Iterator[Job]:
        return iter(self.jobs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(map(repr, self))})"

    def __str__(self):
        parts = map(str, self)
        return f"Job pool [parallel group: {self.parallel_group}]:\n> " + "\n> ".join(parts)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, JobPool):
            return NotImplemented
        else:
            return all(self_job == other_job for self_job, other_job in zip(self, other, strict=False))

    def __hash__(self) -> int:
        return hash(self.parallel_group)


class CallableJob(Job):
    def __init__(
        self,
        name: str,
        executable: Callable[..., Any] | str,
        parallel_group: str | None = None,
        args: tuple[Any, ...] | list[Any] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(name, executable, parallel_group)
        self.args = tuple(args)
        self.kwargs = kwargs or {}

        self.validate_and_prepare_executable()

    def execute(self) -> Any:
        return self.executable(*self.args, **self.kwargs)

    def validate_and_prepare_executable(self) -> None:
        if isinstance(self.executable, str):
            try:
                module_path, executable = self.executable.rsplit(".", 1)
            except ValueError as e:
                raise ValueError(
                    f"Invalid value for 'executable': {self.executable!r}."
                    f"When passing a string, it must be in the format: "
                    f"'<package>.<module>.<function>',"
                    f" e.g. 'my_package.mysubpackage.myscript.myfunction'."
                ) from e

            if importlib.util.find_spec(module_path) is None:
                raise ModuleNotFoundError(f"Module '{module_path}' not found")

            module = importlib.import_module(module_path)
            executable = getattr(module, executable)
            self.executable = executable

        if not callable(self.executable):
            raise TypeError(
                "'executable' must be either a callable object or an import path pointing to a callable object."
            )

    def __getstate__(self):
        state = super().__getstate__()
        state.update({"args": self.args, "kwargs": self.kwargs})
        return state

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(name={self.name!r},"
            f" executable={self.executable},"
            f" parallel_group={self.parallel_group!r},"
            f" args={self.args!r},"
            f" kwargs={self.kwargs!r})"
        )

    def __str__(self) -> str:
        try:
            callable_name = self.executable.__name__
        except AttributeError:
            callable_name = f"{self.__class__.__name__}.__call__"

        parts = [repr(arg) for arg in self.args]
        parts += [f"{k}={v!r}" for k, v in self.kwargs.items()]
        args_kwargs_str = ", ".join(parts)
        return f"{callable_name}({args_kwargs_str})"


class AsyncCallableJob(CallableJob):
    def __init__(
        self,
        name: str,
        executable: Callable[..., Any] | str,
        parallel_group: str | None = None,
        args: tuple[Any, ...] | list[Any] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            name,
            executable,
            parallel_group,
            args,
            kwargs,
        )

        if not iscoroutinefunction(self.executable):
            raise TypeError("'executable' must be an async function (defined with 'async def')")

    def execute(self) -> Any:
        result = asyncio.run(self.async_execute())
        return result

    async def async_execute(self) -> Any:
        result = await self.executable(*self.args, **self.kwargs)
        return result


class ScriptJob(Job):
    def __init__(self, name: str, executable: str, parallel_group: str | None = None) -> None:
        super().__init__(name, executable, parallel_group)

        # Validation step:
        # Ensure `executable` is a string and determine whether it represents
        # a filesystem script (.py file) or an importable Python module.
        # - If it's a script: resolve its absolute path (strict=True ensures it exists).
        # - If it's a module: verify that it can be imported via importlib.util.find_spec().

        if not isinstance(self.executable, (str, Path)):
            raise TypeError(
                "'executable' must be a pathlib.Path object or string in one of the following formats:\n"
                "  - <package>.<module>\n"
                "  - /Users/myproject/mysubfolder/myscript.py"
            )

        if str(self.executable).endswith(".py"):
            try:
                self.executable_path = Path(self.executable).resolve(strict=True)
            except FileNotFoundError:
                raise FileNotFoundError(
                    "File not found. Relative paths are resolved relative to the current working directory."
                ) from None
            self.is_script = True
        else:
            if importlib.util.find_spec(self.executable) is None:
                raise ModuleNotFoundError(f"Module '{self.executable}' not found")
            self.is_script = False
            self.executable_path = None  # no path

    def execute(self) -> Any:
        exit_code = 0
        globals_after_run = {}
        try:
            if self.is_script:
                globals_after_run = runpy.run_path(str(self.executable_path), run_name="__main__")
            else:
                globals_after_run = runpy.run_module(self.executable, run_name="__main__", alter_sys=True)
        except SystemExit as e:
            exit_code = e.code if isinstance(e.code, int) else 1
        finally:
            if exit_code != 0:
                raise RuntimeError(
                    f"Module or script '{self.executable_path or self.executable}'"
                    f" exited with non-zero code {exit_code}"
                ) from None

        return globals_after_run

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(name={self.name!r},"
            f" executable={self.executable!r},"
            f" parallel_group={self.parallel_group!r})"
        )

    def __str__(self) -> str:
        executable_name = self.executable_path or self.executable
        return str(executable_name)


# ---------------------------------------------------------------------------
# Factory Design Pattern — Job Factory(singledispatch-like factory function)
# ---------------------------------------------------------------------------


@Dispatcher
def create_job(
    job_type: str,
    /,
    name: str,
    executable: str | Callable[..., Any],
    parallel_group: str | None = None,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
    **extras: dict[str, Any],
) -> Job:
    """
    Factory function to create Job instances of different types.

    This function dispatches creation to the appropriate registered job
    constructor based on `task_type`. Supported job types are defined in
    `SUPPORTED_JOB_TYPES`.

    Parameters:
        job_type (str): The type of job to create (e.g., "callable", "async_callable", "script").
        name (str): The name of the job.
        executable (str | Callable[..., Any]): The executable associated with the job.
        parallel_group (str | None): Optional name for the job's parallel group.
        args (tuple[Any, ...]): Positional arguments to pass to the executable (for callable jobs).
        kwargs (dict[str, Any] | None): Keyword arguments to pass to the executable (for callable jobs).

    Returns:
        Job: An instance of the requested job type.

    Raises:
        ValueError: If `task_type` is not supported. For custom job types, create a
        subclass of `Job` and register it using `create_job.register`.

    Example:
        class MyJob(Job):
            ...

        @create_job.register('my_job')
        def create_my_job(...):
            return MyJob(...)
    """
    raise ValueError(
        f"Invalid 'task_type': {job_type}. Must be one of {SUPPORTED_JOB_TYPES}. "
        f"For custom jobs, subclass `Job` and register it using `create_job.register`."
    )


@create_job.register("callable")
def create_callable_job(
    job_type: str,
    name: str,
    executable: Callable[..., Any],
    parallel_group: str | None = None,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
) -> CallableJob:
    return CallableJob(name=name, executable=executable, parallel_group=parallel_group, args=args, kwargs=kwargs)


@create_job.register("async_callable")
def create_async_callable_job(
    job_type: str,
    name: str,
    executable: Callable[..., Any],
    parallel_group: str | None = None,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
) -> AsyncCallableJob:
    return AsyncCallableJob(name=name, executable=executable, parallel_group=parallel_group, args=args, kwargs=kwargs)


@create_job.register("script")
def create_script_job(
    job_type: str, name: str, executable: str, parallel_group: str | None = None, **extras
) -> ScriptJob:
    return ScriptJob(name=name, executable=executable, parallel_group=parallel_group)
