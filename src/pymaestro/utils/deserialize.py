# src/pymaestro/utils/deserialize.py
import importlib
from datetime import date, datetime
from types import FunctionType
from typing import Any

from ..job_registry import JobRegistry
from ..jobs import Job, JobPool, create_job
from .dispatcher import Dispatcher
from .wrappers import DependsOn, Resource

__all__ = ["deserialize"]

# ---------------------------------------------------------------------------
#  Deserialization — singledispatch-like factory
# ---------------------------------------------------------------------------


def get_type(obj: dict[str, Any]) -> Any:
    return obj.get("type", object())


@Dispatcher(key_generator=get_type)
def deserialize(obj: Any) -> Any:
    return obj


@deserialize.register("datetime")
def deserialize_datetime(obj: dict[str, str]) -> datetime:
    return datetime.fromisoformat(obj["value"])


@deserialize.register("date")
def deserialize_date(obj: dict[str, str]) -> date:
    return date.fromisoformat(obj["value"])


@deserialize.register("function")
def deserialize_function(obj: dict[str, str]) -> FunctionType:
    import_path, function_name = obj["value"].rsplit(".", maxsplit=1)
    module = importlib.import_module(import_path)
    return getattr(module, function_name)


@deserialize.register("callable")
@deserialize.register("async_callable")
@deserialize.register("script")
def deserialize_callable_job(obj: dict[str, str]) -> Job:
    _type = obj.pop("type")  # it must be passed as positional argument to the create_job factory function
    return create_job(_type, **obj)


@deserialize.register("job_pool")
def deserialize_job_pool(obj: dict[str, Any]) -> JobPool:
    return JobPool(*obj.get("jobs", []))


@deserialize.register("JobRegistry")
def deserialize_job_registry(obj: dict[str, Any]) -> JobRegistry:
    return JobRegistry(obj.get("value", []))


@deserialize.register("DependsOn")
def deserialize_depends_on(obj: dict):
    return DependsOn(obj["value"])


@deserialize.register("Resource")
def deserialize_resource(obj: dict):
    return Resource(obj["generator_fn"], obj["generator_args"], obj["generator_kwargs"])
