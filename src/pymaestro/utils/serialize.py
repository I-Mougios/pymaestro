# src/pymaestro/utils/serialize.py
import json
from collections.abc import Iterable
from datetime import date, datetime
from functools import singledispatch
from inspect import iscoroutinefunction
from pathlib import Path
from types import FunctionType
from typing import Any

from ..jobs import CallableJob, JobPool, ScriptJob
from .wrappers import DependsOn, Resource

__all__ = ["serialize"]
# ---------------------------------------------------------------------------
#  Serialization — singledispatch-based JSON encoding
# ---------------------------------------------------------------------------


@singledispatch
def serialize(obj: Any) -> Any:
    return json.dumps(obj)


@serialize.register(Iterable)
def serialize_iterable(obj: Iterable[Any]) -> dict[str, Any]:
    return {"type": obj.__class__.__name__, "value": list(obj)}


@serialize.register
def serialize_date(obj: date) -> dict[str, str]:
    return {"type": "date", "value": obj.isoformat()}


@serialize.register(datetime)
def serialize_datetime(value: datetime) -> dict[str, Any]:
    return {"type": "datetime", "value": value.isoformat()}


@serialize.register(JobPool)
def serialize_job_pool(obj: JobPool) -> dict[str, Any]:
    return {"type": "job_pool", "jobs": list(obj)}


@serialize.register(Path)
def serialize_path(obj: Path) -> str:
    return str(obj)


@serialize.register(FunctionType)
def serialize_function(obj: FunctionType) -> dict[str, str]:
    return {"type": "function", "value": f"{obj.__module__}.{obj.__qualname__}"}


@serialize.register(ScriptJob)
def serialize_script_job(obj: ScriptJob) -> dict[str, Any]:
    return {"type": "script", "name": obj.name, "executable": obj.executable, "parallel_group": obj.parallel_group}


@serialize.register(CallableJob)
def serialize_callable_job(obj: CallableJob) -> dict[str, Any]:
    return {
        "type": "async_callable" if iscoroutinefunction(obj.executable) else "callable",
        "name": obj.name,
        "executable": obj.executable,  # by default handle case where executable is function
        "parallel_group": obj.parallel_group,
        "args": obj.args,
        "kwargs": obj.kwargs,
    }


@serialize.register(DependsOn)
def serialize_depends_on(dep: DependsOn):
    return {"type": "DependsOn", "value": dep.name}


@serialize.register(Resource)
def serialize_resource(dep: Resource):
    return {
        "type": "Resource",
        "generator_fn": dep.generator_fn,
        "generator_args": dep.generator_args,
        "generator_kwargs": dep.generator_kwargs,
    }
