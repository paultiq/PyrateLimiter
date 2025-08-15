# ruff: noqa: T201
import asyncio
import logging
import time
from datetime import datetime

from pyrate_limiter import Limiter
from pyrate_limiter.limiter_factory import create_inmemory_limiter

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


async def ticker():
    for _ in range(10):
        logger.info("[TICK] %s", datetime.now())
        await asyncio.sleep(0.5)


def mapping(name, weight, i):
    return "mytask", 1


async def test_asyncio_ratelimit():
    logger.info("Running task_async using try_acquire_async")
    logger.info("Note that the TICKs continue while the tasks are waiting")

    start = time.time()
    limiter = create_inmemory_limiter()

    async def task_async(name, weight, i, limiter: Limiter):
        await limiter.try_acquire_async(name, weight)

        logger.info("try_acquire_async: %s, %s:%s", datetime.now(), name, weight)

    await asyncio.gather(ticker(), *[task_async(str(i), 1, i, limiter) for i in range(10)])
    logger.info("Run 10 calls in %d sec", time.time() - start)


if __name__ == "__main__":
    asyncio.run(test_asyncio_ratelimit())
