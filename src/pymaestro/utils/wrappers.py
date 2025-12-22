# src/pymaestro/utils/wrappers.py
import inspect
import warnings

import wrapt

__all__ = ["DependsOn", "Resource", "is_completed", "inject_dependencies"]


class DependsOn:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"Depends(name={self.name})"


class Resource:
    def __init__(self, generator_fn, generator_args=None, generator_kwargs=None):
        if not inspect.isgeneratorfunction(generator_fn):
            raise TypeError("'generator_fn' must be a generator function")

        self.generator_fn = generator_fn
        self.generator_args = generator_args or ()
        self.generator_kwargs = generator_kwargs or {}


@wrapt.decorator
def is_completed(wrapped, instance, args, kwargs):
    if hasattr(instance, "is_completed") and instance.is_completed:
        warnings.warn(
            f"'{instance.name}' was called but the job has already been completed.",
            category=RuntimeWarning,
            stacklevel=4,
        )
        return instance.result

    output = wrapped(*args, **kwargs)
    instance.is_completed = True
    instance.result = output
    return output


@wrapt.decorator
def inject_dependencies(wrapped, instance, args, kwargs):
    active_generators = []

    # Normalize args / kwargs
    orig_args = tuple(getattr(instance, "args", ()))
    orig_kwargs = dict(getattr(instance, "kwargs", {}))

    def resolve(value):
        if isinstance(value, Resource):
            generator = value.generator_fn(*value.generator_args, **value.generator_kwargs)
            active_generators.append(generator)
            return next(generator)
        return value

    try:
        if hasattr(instance, "args"):
            instance.args = [resolve(arg) for arg in orig_args]

        if hasattr(instance, "kwargs"):
            instance.kwargs = {k: resolve(v) for k, v in orig_kwargs.items()}

        return wrapped(*args, **kwargs)

    finally:
        for gen in reversed(active_generators):
            gen.close()

        if hasattr(instance, "args"):
            instance.args = orig_args

        if hasattr(instance, "kwargs"):
            instance.kwargs = orig_kwargs
