# tests/test_job_registry.py
import itertools
import re

import pytest

from pymaestro.job_registry import JobRegistry
from pymaestro.jobs import Job, JobPool


def test_init(simple_math_jobs: list[Job]) -> None:
    JobRegistry(simple_math_jobs)


def test_init_raise_type_error(simple_math_jobs: list[Job]) -> None:
    simple_math_jobs += "a"
    with pytest.raises(TypeError, match=re.escape("Elements passed to JobRegistry must be instances of 'Job'")):
        JobRegistry(simple_math_jobs)


def test_append(simple_math_jobs: list[Job], download_images_script_job) -> None:
    job_registry = JobRegistry(simple_math_jobs)
    job_registry.append(download_images_script_job)
    assert job_registry[-1] == download_images_script_job


def test_setitem(simple_math_jobs: list[Job], download_images_script_job) -> None:
    job_registry = JobRegistry(simple_math_jobs)
    job_registry[0] = download_images_script_job
    assert job_registry[0] == download_images_script_job


def test_insert(simple_math_jobs: list[Job], download_images_script_job) -> None:
    job_registry = JobRegistry(simple_math_jobs)
    job_registry.insert(1, download_images_script_job)
    assert job_registry[1] == download_images_script_job


def test_remove(simple_math_jobs: list[Job]) -> None:
    first_job_name = simple_math_jobs[0].name
    job_registry = JobRegistry(simple_math_jobs)
    job_registry.remove(first_job_name)
    assert len(job_registry) == len(simple_math_jobs) - 1
    assert first_job_name not in job_registry.unique_names


def test_remove_raise_key_error(simple_math_jobs: list[Job]) -> None:
    job_registry = JobRegistry(simple_math_jobs)
    with pytest.raises(KeyError, match="Job with 'job_name' not found in registry"):
        job_registry.remove("job_name")


def test_pop(simple_math_jobs: list[Job], download_images_script_job) -> None:
    job_registry = JobRegistry(simple_math_jobs)
    job_registry.append(download_images_script_job)
    last_job = job_registry.pop()
    assert last_job == download_images_script_job
    assert last_job.name == download_images_script_job.name
    assert last_job.name not in job_registry.unique_names

    second_job = job_registry.pop(1)
    assert second_job == simple_math_jobs[1]
    assert second_job.name == simple_math_jobs[1].name
    assert second_job.name not in job_registry.unique_names


def test_pop_raise_index_error(download_images_script_job: Job) -> None:
    with pytest.raises(IndexError, match="Job with 'index -1' not found in registry"):
        job_registry = JobRegistry([download_images_script_job])
        job_registry.pop()
        job_registry.pop()


def test_contains(simple_math_jobs: list[Job]) -> None:
    job_registry = JobRegistry(simple_math_jobs)
    first_job_name = simple_math_jobs[0].name
    assert first_job_name in job_registry
    job_registry.remove(first_job_name)
    assert first_job_name not in job_registry


def test_index(simple_math_jobs: list[Job]) -> None:
    task_registry = JobRegistry(simple_math_jobs)
    for i, job in enumerate(task_registry):
        assert task_registry.index(job.name) == i


def test_index_raise_key_error(simple_math_jobs: list[Job]) -> None:
    task_registry = JobRegistry(simple_math_jobs)
    with pytest.raises(KeyError, match="Job with name 'job_name' not found in registry"):
        assert task_registry.index("job_name")


def test_registry_grouped_jobs(
    simple_math_jobs: list[Job],
    download_images_script_job: Job,
    advance_math_jobs,
    cook_vegetables_async_callable_job,
    simple_math_pool,
    advance_math_pool,
) -> None:
    task_registry = JobRegistry([download_images_script_job])
    task_registry.extend(advance_math_jobs)
    task_registry.append(cook_vegetables_async_callable_job)
    task_registry.extend(simple_math_jobs)

    assert len(task_registry.grouped_jobs) == 4
    assert list(task_registry.grouped_jobs.values()) == list(range(4))

    assert task_registry.grouped_jobs[download_images_script_job] == 0
    assert task_registry.grouped_jobs[advance_math_pool] == 1
    assert task_registry.grouped_jobs[cook_vegetables_async_callable_job] == 2
    assert task_registry.grouped_jobs[simple_math_pool] == 3


def test_reset_cached(
    simple_math_jobs: list[Job],
    download_images_script_job,
    advance_math_jobs: list[Job],
    simple_math_pool: JobPool,
    advance_math_pool: JobPool,
) -> None:
    job_registry = JobRegistry(simple_math_jobs)
    job_registry.extend(advance_math_jobs)
    assert job_registry.grouped_jobs[simple_math_pool] == 0
    assert job_registry.grouped_jobs[advance_math_pool] == 1
    job_registry.insert(1, download_images_script_job)
    assert job_registry._grouped_jobs is None
    assert job_registry.grouped_jobs[simple_math_pool] == 0
    assert job_registry.grouped_jobs[download_images_script_job] == 1
    assert job_registry.grouped_jobs[advance_math_pool] == 2


def test_swap_by_index(all_jobs: list[Job]) -> None:
    for perm in itertools.permutations(range(len(all_jobs)), r=2):
        registry = JobRegistry(all_jobs)
        i = perm[0]
        j, job_j = perm[1], registry[perm[1]]
        registry.swap(i, j)
        assert registry[i] is job_j


def test_swap_by_name(all_jobs: list[Job]) -> None:
    for perm in itertools.permutations((job.name for job in all_jobs), r=2):
        registry = JobRegistry(all_jobs)
        i_job_name, j_job_name = perm[0], perm[1]
        i_job_idx = registry.index(i_job_name)
        registry.swap(i_job_name, j_job_name)
        assert registry[i_job_idx].name == j_job_name


def test_swap_mix_idx_name(all_jobs: list[Job]) -> None:
    for (i, _), (j, job_name_j) in itertools.permutations(((idx, job.name) for idx, job in enumerate(all_jobs)), r=2):
        registry = JobRegistry(all_jobs)
        job_j = registry[j]
        registry.swap(i, job_name_j)
        assert registry[i] == job_j
