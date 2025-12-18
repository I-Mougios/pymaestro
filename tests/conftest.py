# tests/conftest.py
from pathlib import Path
from typing import Generator

import pytest

from pymaestro.jobs import Job, JobPool, create_job
from tests.helper_scripts.download_images import remove_dir
from tests.helper_scripts.functions import (
    FactorialJob,
    FibonacciJob,
    PrimeCheckJob,
    approximate_pi,
    cook_vegetables,
    matrix_multiply,
    sum_of_squares,
)


@pytest.fixture(scope="function")
def download_folder() -> Generator[Path, None, None]:
    folder = Path(__file__).resolve().parent / "raw_images"
    folder.mkdir(parents=True, exist_ok=True)
    yield folder
    # teardown
    remove_dir(folder)


@pytest.fixture(scope="function")
def processed_images_folder() -> Generator[Path, None, None]:
    folder = Path(__file__).resolve().parent / "processed_images"
    folder.mkdir(parents=True, exist_ok=True)
    yield folder
    # teardown
    remove_dir(folder)


# -------------------------
# SCRIPT JOB FIXTURES
# -------------------------


@pytest.fixture(scope="function")
def download_images_script_job() -> Job:
    script_path = Path(__file__).resolve().parent / "helper_scripts" / "download_images.py"
    download_images_script_job = create_job("script", name="download_images", executable=str(script_path))
    return download_images_script_job


@pytest.fixture(scope="function")
def script_job_with_exit_code_zero() -> Job:
    script_path = Path(__file__).resolve().parent / "helper_scripts" / "script_with_exit_code_zero.py"
    script_job_with_exit_code_zero = create_job("script", name="exit_code_zero", executable=str(script_path))
    return script_job_with_exit_code_zero


@pytest.fixture(scope="function")
def script_job_with_exit_code_other_than_zero() -> Job:
    script_path = Path(__file__).resolve().parent / "helper_scripts" / "script_with_exit_code_other_than_zero.py"
    script_job_with_exit_code_other_than_zero = create_job(
        "script", name="exit_code_other_than_zero", executable=str(script_path)
    )
    return script_job_with_exit_code_other_than_zero


@pytest.fixture(scope="function")
def script_job_with_no_exit_code() -> Job:
    script_path = Path(__file__).resolve().parent / "helper_scripts" / "script_with_no_exit_code.py"
    script_job_with_no_exit_code = create_job("script", name="no_exit_code", executable=str(script_path))
    return script_job_with_no_exit_code


# -------------------------
# SIMPLE MATH FIXTURES
# -------------------------


@pytest.fixture(scope="function")
def simple_math_jobs() -> list[Job]:
    """Generator fixture yielding each simple math Job one by one."""
    jobs = [
        create_job(
            "callable",
            name="ten first fibonacci numbers",
            executable=FibonacciJob(),
            args=(10,),
            parallel_group="simple_math",
        ),
        create_job(
            "callable", name="sum of squares", executable=sum_of_squares, args=(10,), parallel_group="simple_math"
        ),
        create_job(
            "callable", name="factorial of 10", executable=FactorialJob(), args=(10,), parallel_group="simple_math"
        ),
    ]
    return jobs


@pytest.fixture(scope="function")
def simple_math_pool(simple_math_jobs) -> JobPool:
    """Fixture returning a JobPool with all simple math Jobs."""
    return JobPool(*simple_math_jobs)


# -------------------------
# ADVANCE MATH FIXTURES
# -------------------------


@pytest.fixture(scope="function")
def advance_math_jobs() -> list[Job]:
    """Generator fixture yielding each advance math Job one by one."""
    jobs = [
        create_job(
            "callable",
            name="is 10 a prime number",
            executable=PrimeCheckJob(),
            args=(10,),
            parallel_group="advance_math",
        ),
        create_job(
            "callable",
            name="multiply 15X15 matrices",
            executable=matrix_multiply,
            args=(15,),
            parallel_group="advance_math",
        ),
        create_job(
            "callable",
            name="approximate pi with 100 iterations",
            executable=approximate_pi,
            args=(100,),
            parallel_group="advance_math",
        ),
    ]
    return jobs


@pytest.fixture(scope="function")
def advance_math_pool() -> JobPool:
    """Fixture returning a JobPool with all advance math Jobs."""
    jobs = [
        create_job(
            "callable",
            name="is 10 a prime number",
            executable=PrimeCheckJob(),
            args=(10,),
            parallel_group="advance_math",
        ),
        create_job(
            "callable",
            name="multiply 15X15 matrices",
            executable=matrix_multiply,
            args=(15,),
            parallel_group="advance_math",
        ),
        create_job(
            "callable",
            name="approximate pi with 100 iterations",
            executable=approximate_pi,
            args=(100,),
            parallel_group="advance_math",
        ),
    ]
    return JobPool(*jobs)


# -------------------------
# ASYNC CALLABLES FIXTURES
# -------------------------


@pytest.fixture(scope="function")
def cook_vegetables_async_callable_job() -> Job:
    cook_vegetables_job = create_job("async_callable", name="cook vegetables", executable=cook_vegetables)
    return cook_vegetables_job


# -----------------------
# LIST OF JOBS
# -----------------------


@pytest.fixture(scope="function")
def all_jobs(
    simple_math_jobs: Generator[Job, None, None],
    advance_math_jobs: Generator[Job, None, None],
    download_images_script_job: Job,
    cook_vegetables_async_callable_job: Job,
) -> list[Job]:
    math_jobs = [*simple_math_jobs, *advance_math_jobs]

    jobs = math_jobs + [download_images_script_job, cook_vegetables_async_callable_job]
    return jobs
