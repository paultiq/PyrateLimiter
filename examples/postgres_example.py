"""
May need to install the psycopg binaries:
# uv pip install psycopg[binary]

"""

import logging
import time
from datetime import datetime
from typing import List

from psycopg_pool import ConnectionPool as PgConnectionPool

from pyrate_limiter import Duration, Limiter, PostgresBucket, Rate

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)


def create_postgres_bucket(rates: List[Rate]):
    pool = PgConnectionPool("postgresql://postgres:postgres@localhost:5432")
    table = f"test_bucket_{int(time.time())}"
    bucket = PostgresBucket(pool, table, rates)
    assert bucket.count() == 0
    return bucket


def test_postgres():
    rates = [Rate(3, Duration.SECOND * 3)]

    with create_postgres_bucket(rates) as postgres_bucket:
        with Limiter(postgres_bucket, raise_when_fail=False, max_delay=Duration.DAY) as limiter:

            def task(name, weight):
                acquired = limiter.try_acquire(name, weight)
                logger.info("%s %s: %s, %s", datetime.now(), name, weight, acquired)

            for i in range(10):
                task(str(i), 1)


if __name__ == "__main__":
    logger.info("To start a postgres container: ")
    logger.info("# docker run --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres")

    test_postgres()
