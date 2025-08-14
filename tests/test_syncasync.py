import asyncio

import pytest

from pyrate_limiter import Limiter
from pyrate_limiter import limiter_factory


def run_sync_task(limiter: Limiter, num: int, results: list[int]):
    for i in range(num):
        limiter.try_acquire("foo")
        results.append(i)


def run_async_task(limiter: Limiter, num: int, results: list[int]):
    async def task_async(_limiter: Limiter, name, weight, results: list[int]):
        while not await _limiter.try_acquire_async(name, weight):
            pass
        results.append(num)

    async def run_many_async_tasks(results: list[int]):
        return await asyncio.gather(
            *(task_async(limiter, str(i), 1, results) for i in range(num))
        )

    asyncio.run(run_many_async_tasks(results))


def test_sync_async():
    limiter = limiter_factory.create_inmemory_limiter(rate_per_duration=10)

    results = []
    run_sync_task(limiter, 1, results)
    run_async_task(limiter, 1, results)
    run_sync_task(limiter, 1, results)
    run_async_task(limiter, 1, results)

    assert len(results) == 4


def test_async_sync():
    limiter = limiter_factory.create_inmemory_limiter(rate_per_duration=10)

    results = []
    run_async_task(limiter, 1, results)
    run_sync_task(limiter, 1, results)
    run_async_task(limiter, 1, results)
    run_sync_task(limiter, 1, results)
    run_async_task(limiter, 1, results)

    assert len(results) == 5


def test_sync_async_async_bucket():
    limiter = limiter_factory.create_inmemory_limiter(
        rate_per_duration=10, async_wrapper=False
    )
    results: list[int] = []

    run_async_task(limiter, 1, results)
    run_sync_task(limiter, 1, results)
    run_async_task(limiter, 1, results)
    run_sync_task(limiter, 1, results)
    run_async_task(limiter, 1, results)

    assert len(results) == 5


def test_async_wrapper_outside_async():
    with pytest.raises(RuntimeError):
        limiter_factory.create_inmemory_limiter(
            rate_per_duration=10, async_wrapper=True
        )


def test_async_sync_async_bucket():
    limiter = limiter_factory.create_inmemory_limiter(rate_per_duration=10)

    results = []
    run_async_task(limiter, 1, results)
    run_async_task(limiter, 1, results)
    run_async_task(limiter, 1, results)

    assert len(results) == 3
