# tests/helper_scripts/functions.py
import asyncio
import math
import os
import random
import time

# -------------------------
# 1. CALLABLE TASKS
# -------------------------


def sum_of_squares(n: int) -> int:
    """Computationally heavy: sum of squares from 1 to n."""
    print(f"[sum_of_squares] PID={os.getpid()} Calculating sum of squares up to {n}")  # noqa: T201
    n = int(n)
    return sum(i * i for i in range(1, n + 1))


class FactorialJob:
    """Callable task that calculates factorial of a number."""

    def __call__(self, n: int) -> int:
        print(f"[FactorialTask] Calculating factorial of {n}")  # noqa: T201
        return math.factorial(n)


class FibonacciJob:
    """Callable task that returns Fibonacci sequence up to n."""

    def __call__(self, n: int) -> list[int]:
        print(f"[FibonacciTask] Generating Fibonacci sequence up to {n}")  # noqa: T201
        sequence = [0, 1]
        while len(sequence) < n:
            sequence.append(sequence[-1] + sequence[-2])
        time.sleep(0.5)
        return sequence[:n]


# -------------------------
# 2. ASYNC TASKS
# -------------------------


async def boil_water() -> str:
    """Async task: simulates boiling water."""
    print("[boil_water] Putting kettle on...")  # noqa: T201
    await asyncio.sleep(1)
    print("[boil_water] Water is boiled!")  # noqa: T201
    return "Boiled water ready"


async def cut_vegetables() -> str:
    """Async task: simulates cutting vegetables."""
    print("[cut_vegetables] Cutting vegetables...")  # noqa: T201
    await asyncio.sleep(0.5)
    print("[cut_vegetables] Vegetables are ready!")  # noqa: T201
    return "Vegetables ready"


async def cook_vegetables() -> list[str]:
    """Async task: simulates cooking vegetables."""
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(boil_water()), tg.create_task(cut_vegetables())]

    return [task.result() for task in tasks]


# -------------------------
# 3. MULTIPROCESSING TASKS
# -------------------------


class PrimeCheckJob:
    """Callable task that checks if a number is prime."""

    def __call__(self, n: int) -> bool:
        print(f"[PrimeCheckTask] Checking if {n} is prime")  # noqa: T201
        if n <= 1:
            return False
        for i in range(2, int(math.sqrt(n)) + 1):
            if n % i == 0:
                return False
        return True


def matrix_multiply(size: int) -> list[list[int]]:
    """Computationally expensive: multiply two size x size matrices."""
    size = int(size)
    print(f"[matrix_multiply] PID={os.getpid()} Multiplying {size}x{size} matrices")  # noqa: T201
    A = [[random.randint(1, 10) for _ in range(size)] for _ in range(size)]
    B = [[random.randint(1, 10) for _ in range(size)] for _ in range(size)]

    # Result matrix
    result = [[0] * size for _ in range(size)]
    for i in range(size):
        for j in range(size):
            for k in range(size):
                result[i][j] += A[i][k] * B[k][j]
    return result


def approximate_pi(iterations: int) -> float:
    """Approximates Pi using the Leibniz series."""
    iterations = int(iterations)
    print(f"[approximate_pi] PID={os.getpid()} Approximating Pi with {iterations} iterations")  # noqa: T201
    pi_approx = 0.0
    for k in range(iterations):
        pi_approx += ((-1) ** k) / (2 * k + 1)
    return 4 * pi_approx
