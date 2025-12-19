import asyncio
import re
from pathlib import Path

import pytest

from pymaestro import DependsOn, Maestro
from pymaestro.jobs import AsyncCallableJob, CallableJob

maestro = Maestro()


def test_add_simple_decorator():
    @maestro.add  # No parenthesis
    def my_func():
        return "Job done"

    assert "my_func" in maestro.registry
    assert isinstance(maestro.registry[0], CallableJob)

    @maestro.add()  # with parenthesis
    async def my_async_func():
        await asyncio.sleep(0.1)
        return "Job done"

    assert "my_async_func" in maestro.registry
    assert isinstance(maestro.registry[1], AsyncCallableJob)
    maestro.registry.clear()


def test_add_decorator_factory():
    @maestro.add(
        name="custom_job_name",
        args=("first_argument",),
        kwargs={"kw1": "second_argument"},
        parallel_group="parallel_group",
    )
    def func_with_args_and_kwargs(arg, *, kw1):
        return f"Job done with {arg} and {kw1=}"

    assert "custom_job_name" in maestro.registry
    assert isinstance(maestro.registry[0], CallableJob)
    assert maestro.registry[0].args == ("first_argument",)
    assert maestro.registry[0].kwargs == {"kw1": "second_argument"}
    assert maestro.registry[0].parallel_group == "parallel_group"

    @maestro.add(
        name="custom_name_for_async_job",
        args=("first_argument",),
        kwargs={"kw1": "second_argument"},
        parallel_group="parallel_group",
    )
    async def async_func_with_args_and_kwargs(arg, *, kw1):
        await asyncio.sleep(0.1)
        return f"Job done with {arg} and {kw1=}"

    assert "custom_name_for_async_job" in maestro.registry
    assert isinstance(maestro.registry[1], AsyncCallableJob)
    assert maestro.registry[1].args == ("first_argument",)
    assert maestro.registry[1].kwargs == {"kw1": "second_argument"}
    assert maestro.registry[1].parallel_group == "parallel_group"
    maestro.registry.clear()


def test_add_direct_invocation():
    module_path = maestro.add(
        "tests.helper_scripts.script_with_exit_code_zero",
        job_type="script",
        name="script_with_exit_code_zero_as_module",
    )

    assert "script_with_exit_code_zero_as_module" in maestro.registry
    assert module_path == "tests.helper_scripts.script_with_exit_code_zero"

    maestro.add(
        "tests.helper_scripts.process_images.process_single_image",
        job_type="callable",
        name="process_images_from_import_path",
        kwargs={"orig_path": Path() / "raw_images", "save_dir": Path() / "processed_images"},
    )

    assert "process_images_from_import_path" in maestro.registry

    maestro.add(
        "./tests/helper_scripts/script_with_exit_code_zero.py", job_type="script", name="script_with_exit_code_zero"
    )

    assert "script_with_exit_code_zero" in maestro.registry

    maestro.registry.clear()


def test_add_raise_value_error_for_missing_job_type():
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Parameter 'job_type' is required when registering a job by string path "
            "(got executable='tests.helper_scripts.functions.sum_of_squares')."
        ),
    ):
        maestro.add("tests.helper_scripts.functions.sum_of_squares", args=(5,))


def test_dependency_resolution():
    @maestro.add
    def five():
        return 5

    @maestro.add
    def four():
        return 4

    @maestro.add(args=(DependsOn("five"),), kwargs={"b": DependsOn("four")})
    def add(a, b):
        return a + b

    results = maestro.execute()
    assert results[-1] == 9
    maestro.registry.clear()


def test_dependency_resolution_when_dependencies_come_after_the_actual_job():
    @maestro.add(args=(DependsOn("five"),), kwargs={"b": DependsOn("four")})
    def add(a, b):
        return a + b

    @maestro.add
    def five():
        return 5

    @maestro.add
    def four():
        return 4

    with pytest.warns(RuntimeWarning, match=".* was called but the job has already been completed."):
        results = maestro.execute()
        assert results[0] == 9

    maestro.registry.clear()


def test_dependency_resolution_raise_key_error():
    @maestro.add(args=(DependsOn("five"),), kwargs={"b": DependsOn("four")})
    def add(a, b):
        return a + b

    with pytest.raises(KeyError, match="Job with name 'five' not found in registry"):
        maestro.execute()

    maestro.registry.clear()
