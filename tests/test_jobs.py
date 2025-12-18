# tests/test_jobs.py
import asyncio
import os
import pickle
import re
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from pymaestro.jobs import AsyncCallableJob, CallableJob, Job, JobPool, ScriptJob, create_job
from tests.helper_scripts.functions import cook_vegetables, sum_of_squares


def test_create_callable_job_using_keywords_arguments() -> None:
    sum_of_squares_job = create_job("callable", name="sum of square to 5", executable=sum_of_squares, args=(5,))
    assert isinstance(sum_of_squares_job, Job)
    assert isinstance(sum_of_squares_job, CallableJob)


def test_create_create_job_using_positional_arguments() -> None:
    sum_of_squares_job = create_job("callable", "sum of square to 5", sum_of_squares, None, (5,))
    assert isinstance(sum_of_squares_job, Job)
    assert isinstance(sum_of_squares_job, CallableJob)
    assert sum_of_squares_job.args == (5,)


def test_create_job_using_keywords_arguments_for_the_callable() -> None:
    sum_of_squares_job = create_job("callable", "sum of square to 5", sum_of_squares, kwargs={"n": 5})
    assert isinstance(sum_of_squares_job, Job)
    assert isinstance(sum_of_squares_job, CallableJob)
    assert sum_of_squares_job.kwargs == {"n": 5}


def test_create_async_callable_job_using_keywords_arguments() -> None:
    cook_vegetables_job = create_job("async_callable", name="cook_vegetables", executable=cook_vegetables)
    assert isinstance(cook_vegetables_job, Job)
    assert isinstance(cook_vegetables_job, CallableJob)
    assert isinstance(cook_vegetables_job, AsyncCallableJob)


def test_create_async_callable_using_positional_arguments() -> None:
    cook_vegetables_job = create_job("async_callable", "cook_vegetables", cook_vegetables)
    assert isinstance(cook_vegetables_job, Job)
    assert isinstance(cook_vegetables_job, CallableJob)
    assert isinstance(cook_vegetables_job, AsyncCallableJob)
    assert cook_vegetables_job.args == ()


def test_create_script_job(download_images_script_job) -> None:
    assert isinstance(download_images_script_job, Job)
    assert isinstance(download_images_script_job, ScriptJob)


def test_create_job_pool(simple_math_pool) -> None:
    assert isinstance(simple_math_pool, JobPool)


def test_callable_job_pickling():
    job = create_job("callable", name="sum of square to 5", executable=sum_of_squares, args=(5,))
    job.result = sum_of_squares(5)
    job.is_completed = True

    with NamedTemporaryFile(delete=False, mode="wb") as f:
        f_name = f.name
        pickle.dump(job, f)

    with open(f_name, "rb") as f:
        job = pickle.load(f)

    os.remove(f_name)

    assert isinstance(job, CallableJob)
    assert job.is_completed
    assert job.args == (5,)
    assert job.result == sum_of_squares(5)


def test_async_callable_job_pickling():
    job = create_job("async_callable", name="cook_vegetables", executable=cook_vegetables)
    job.result = asyncio.run(cook_vegetables())
    job.is_completed = True

    with NamedTemporaryFile(delete=False, mode="wb") as f:
        f_name = f.name
        pickle.dump(job, f)

    with open(f_name, "rb") as f:
        job = pickle.load(f)

    os.remove(f_name)

    assert isinstance(job, AsyncCallableJob)
    assert job.is_completed
    assert job.result == asyncio.run(cook_vegetables())


def test_script_job_pickling(download_images_script_job):
    job = download_images_script_job
    job.result = None
    job.is_completed = True

    with NamedTemporaryFile(delete=False, mode="wb") as f:
        f_name = f.name
        pickle.dump(job, f)

    with open(f_name, "rb") as f:
        job = pickle.load(f)

    os.remove(f_name)

    assert isinstance(job, ScriptJob)
    assert job.is_completed
    assert job.result is None


def test_script_job_execute_with_no_exit_code(script_job_with_no_exit_code) -> None:
    result = script_job_with_no_exit_code.execute()
    for k, v in {"a": "this if my first variable", "b": "this is my second variable"}.items():
        assert result[k] == v


def test_script_job_execute_with_exit_code_zero(script_job_with_exit_code_zero) -> None:
    result = script_job_with_exit_code_zero.execute()
    assert result == {}


def test_script_job_execute_with_exit_code_other_than_zero(script_job_with_exit_code_other_than_zero) -> None:
    job = script_job_with_exit_code_other_than_zero
    # added re.escape because it failed on Windows machine
    with pytest.raises(
        RuntimeError, match=re.escape(f"Module or script '{job.executable_path}' exited with non-zero code 1")
    ):
        script_job_with_exit_code_other_than_zero.execute()


def test_script_job_with_relative_path() -> None:
    script_job = create_job("script", name="script_job", executable=str(Path("./tests/helper_scripts/functions.py")))
    result = script_job.execute()
    assert "sum_of_squares" in result


def test_script_with_not_existing_file() -> None:
    with pytest.raises(
        FileNotFoundError,
        match="File not found. Relative paths are resolved relative to the current working directory.",
    ):
        create_job("script", name="script_job", executable="./tests/helper_scripts/not_existing_file.py")


@pytest.mark.filterwarnings(
    "ignore:'tests.helper_scripts.functions' found in sys.modules after import of package 'tests.helper_scripts'"
)
def test_script_job_provided_as_module() -> None:
    script_job = create_job("script", name="script_job", executable="tests.helper_scripts.functions")
    result = script_job.execute()
    assert "sum_of_squares" in result


def test_script_job_as_module_with_exit_code_zero() -> None:
    script_job = create_job("script", name="script_job", executable="tests.helper_scripts.script_with_exit_code_zero")
    result = script_job.execute()
    assert result == {}


def test_script_job_as_module_with_exit_code_other_than_zero() -> None:
    script_job = create_job(
        "script", name="script_job", executable="tests.helper_scripts.script_with_exit_code_other_than_zero"
    )
    with pytest.raises(RuntimeError, match=f"Module or script '{script_job.executable}' exited with non-zero code 1"):
        script_job.execute()


def test_callable_job_execute():
    job = create_job("callable", name="sum_of_squares", executable=sum_of_squares, args=(5,))
    assert job.execute() == sum_of_squares(5)


def test_string_as_executable_for_callable_job() -> None:
    job = create_job(
        "callable", name="sum_of_squares", executable="tests.helper_scripts.functions.sum_of_squares", args=(5,)
    )
    assert job.execute() == sum_of_squares(5)


def test_async_callable_job_execute():
    job = create_job("async_callable", name="cooking ..", executable=cook_vegetables)
    assert job.execute() == asyncio.run(cook_vegetables())


def test_string_as_executable_for_async_callable_job() -> None:
    job = create_job("async_callable", name="cooking ..", executable="tests.helper_scripts.functions.cook_vegetables")
    assert job.execute() == asyncio.run(cook_vegetables())


def test_sync_callable_as_async_callable_job():
    with pytest.raises(TypeError, match=re.escape("'executable' must be an async function (defined with 'async def')")):
        create_job("async_callable", name="sum of squares", executable=sum_of_squares, args=(5,))


def test_job_pool_execute_as_submitted(simple_math_pool: JobPool, simple_math_jobs) -> None:
    simple_math_jobs_results = []
    for job in simple_math_jobs:
        simple_math_jobs_results.append(job.execute())

    assert simple_math_jobs_results == list(simple_math_pool.execute(mode="as_submitted"))


def test_pool_name_property(simple_math_pool: JobPool) -> None:
    assert simple_math_pool.name == simple_math_pool.parallel_group


def test_is_completed(cook_vegetables_async_callable_job) -> None:
    result = cook_vegetables_async_callable_job.execute()
    assert cook_vegetables_async_callable_job.is_completed
    assert cook_vegetables_async_callable_job.result is result


def test_is_completed_on_job_pool(simple_math_pool) -> None:
    result = simple_math_pool.execute()
    assert simple_math_pool.is_completed
    assert simple_math_pool.result is result

    for job in simple_math_pool:
        assert job.is_completed is None


def test_is_completed_raise_decorator_raise_warning(simple_math_pool) -> None:
    result = simple_math_pool.execute()
    assert simple_math_pool.is_completed
    assert simple_math_pool.result is result

    with pytest.warns(
        RuntimeWarning, match=re.escape(f"'{simple_math_pool.name}' was called but the job has already been completed.")
    ):
        simple_math_pool.execute()


def test_result_property() -> None:
    job = create_job(
        "callable",
        name="ten first fibonacci numbers",
        executable=sum_of_squares,
        args=(2,),
    )
    assert job.result == 5
