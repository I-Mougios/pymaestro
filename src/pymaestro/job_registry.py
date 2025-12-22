# src/pymaestro/job_registry.py
from itertools import groupby
from types import MappingProxyType
from typing import Any, Iterable, Iterator, Optional

from .jobs import Job, JobPool


class JobRegistry:
    """
    A container for managing and organizing jobs.

    This class stores jobs in an internal list and provides utilities to:
    - Group jobs by their `parallel_group` attribute, maintaining insertion order by default.
    - Reorder jobs flexibly, allowing custom execution sequences.

    Intended as a central registry for job orchestration, making it easier
    to inspect, group, and modify job order before execution.
    """

    def __init__(self, jobs: Optional[Iterable[Job]] = None):
        if jobs is None:
            jobs = []

        self.unique_names = set()
        self._jobs = list(jobs)
        self._grouped_jobs: dict[Job, int] | None = None

        for job in self._jobs:
            self._validate_job(job)

    @property
    def jobs(self) -> list[Job]:
        return self._jobs

    @property
    def grouped_jobs(self) -> MappingProxyType[Job, int] | None:
        if self._grouped_jobs is None:
            self.group_jobs_by_parallel_name()

        return self._grouped_jobs

    def append(self, job: Job) -> None:
        self._validate_job(job)
        self._jobs.append(job)
        self.reset_cached()

    def extend(self, jobs: Iterable[Job]) -> None:
        for job in jobs:
            self.append(job)

    def insert(self, index: int, job: Job) -> None:
        self._validate_job(job)
        self._jobs.insert(index, job)
        self.reset_cached()

    def __setitem__(self, index: int, job: Job) -> None:
        self._validate_job(job)
        self._jobs[index] = job
        self.reset_cached()

    def remove(self, job_name: str) -> None:
        candidate_job = filter(lambda job: job.name == job_name, self.jobs)
        first_occurrence = next(candidate_job, None)
        if first_occurrence is None:
            raise KeyError(f"Job with '{job_name}' not found in registry")

        self.jobs.remove(first_occurrence)
        self.unique_names.remove(first_occurrence.name)
        self.reset_cached()

    def pop(self, index: Optional[int] = -1) -> Job:
        try:
            removed_job = self._jobs.pop(index)
            self.unique_names.remove(removed_job.name)
            self.reset_cached()
        except IndexError as ex:
            raise IndexError(f"Job with 'index {index}' not found in registry") from ex

        return removed_job

    def __getitem__(self, index: int) -> Job:
        return self.jobs[index]

    def __iter__(self) -> Iterator[Job]:
        return iter(self.jobs)

    def __len__(self) -> int:
        return len(self.jobs)

    def group_jobs_by_parallel_name(self) -> None:
        """
        Group jobs by their `parallel_group` attribute and store them in `_grouped_jobs`
        with a normalized priority order (0, 1, 2, ...).

        - Jobs with the same `parallel_group` are combined into a single JobPool.
        - Jobs without a `parallel_group` remain as individual jobs.
        - The resulting dictionary maps each job (or JobPool) to its execution priority.
        """

        # 1. Sort jobs by parallel group name (empty string first for jobs without a group).
        #    enumerate(self) keeps the original index so we can later restore execution order.
        indexed_and_sorted = sorted(enumerate(self), key=lambda indexed_job: indexed_job[1].parallel_group or "")

        # 2. Group jobs by their parallel group name.
        grouped_by_parallel_group = groupby(indexed_and_sorted, key=lambda indexed_job: indexed_job[1].parallel_group)

        grouped_jobs_with_priority = []

        # 3. Build JobPool objects for groups, keep single jobs as-is.
        for parallel_group, jobs_in_group in grouped_by_parallel_group:
            if parallel_group:
                # Convert to list so we can iterate multiple times
                jobs_in_group = list(jobs_in_group)

                # Priority is based on the smallest original index (preserves insertion order)
                priority = min(index for index, _ in jobs_in_group)

                # Extract raw jobs from (index, job) pairs
                raw_jobs = tuple(job for _, job in jobs_in_group)

                # If there is only one job in the group, use it directly; otherwise wrap in a JobPool
                combined_job = raw_jobs[0] if len(raw_jobs) == 1 else JobPool(*raw_jobs)

                grouped_jobs_with_priority.append((combined_job, priority))
            else:
                # Jobs without a parallel group keep their original priority (index)
                for priority, job in jobs_in_group:
                    grouped_jobs_with_priority.append((job, priority))

        # 4. Sort all jobs (or job pools) by priority
        grouped_jobs_with_priority.sort(key=lambda job_priority: job_priority[1])

        # 5. Normalize priorities (make them 0, 1, 2, ... sequential)
        ordered_jobs = (job for job, _ in grouped_jobs_with_priority)
        self._grouped_jobs = MappingProxyType({job: priority for priority, job in enumerate(ordered_jobs)})

    def _validate_job(self, job: Any) -> None:
        if not isinstance(job, Job):
            raise TypeError("Elements passed to JobRegistry must be instances of 'Job'.")
        elif isinstance(job, JobPool):
            raise TypeError(
                "Elements passed to JobRegistry must not be JobPools. "
                "Instead, create a separate Job for each executable and assign them the same parallel_group."
            )
        if job.name in self.unique_names:
            raise ValueError(f"Job with name '{job.name}' already exists.")

        self.unique_names.add(job.name)

    def reset_cached(self):
        self._grouped_jobs = None

    def __contains__(self, job_name: str) -> bool:
        return job_name in self.unique_names

    def clear(self):
        self._jobs.clear()
        self.unique_names.clear()
        self.reset_cached()

    def index(self, job_name: str) -> int:
        for idx, job in enumerate(self.jobs):
            if job.name == job_name:
                return idx

        raise KeyError(f"Job with name '{job_name}' not found in registry")

    def swap(self, idx_or_name_i: int | str, idx_or_name_j: int | str) -> None:
        i = self.index(idx_or_name_i) if isinstance(idx_or_name_i, str) else idx_or_name_i
        j = self.index(idx_or_name_j) if isinstance(idx_or_name_j, str) else idx_or_name_j
        self._jobs[i], self._jobs[j] = self._jobs[j], self._jobs[i]
        self.reset_cached()
