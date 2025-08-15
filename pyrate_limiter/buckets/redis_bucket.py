"""Bucket implementation using Redis"""

from __future__ import annotations

from inspect import isawaitable
from typing import TYPE_CHECKING, Awaitable, List, Optional, Tuple, Union

from ..abstracts import AbstractBucket, AsyncAbstractBucket, Rate, RateItem
from ..utils import id_generator

if TYPE_CHECKING:
    from redis import Redis
    from redis.asyncio import Redis as AsyncRedis


class LuaScript:
    """Scripts that deal with bucket operations"""

    PUT_ITEM = """
    local bucket = KEYS[1]
    local now = ARGV[1]
    local space_required = tonumber(ARGV[2])
    local item_name = ARGV[3]
    local rates_count = tonumber(ARGV[4])

    for i=1,rates_count do
        local offset = (i - 1) * 2
        local interval = tonumber(ARGV[5 + offset])
        local limit = tonumber(ARGV[5 + offset + 1])
        local count = redis.call('ZCOUNT', bucket, now - interval, now)
        local space_available = limit - tonumber(count)
        if space_available < space_required then
            return i - 1
        end
    end

    for i=1,space_required do
        redis.call('ZADD', bucket, now, item_name..i)
    end
    return -1
    """


class RedisBucket(AbstractBucket):
    """A bucket using redis for storing data
    - We are not using redis' built-in TIME since it is non-deterministic
    - In distributed context, use local server time or a remote time server
    - Each bucket instance use a dedicated connection to avoid race-condition
    - can be either sync or async
    """

    rates: List[Rate]
    failing_rate: Optional[Rate]
    bucket_key: str
    script_hash: str
    redis: Redis

    def __init__(
        self,
        rates: List[Rate],
        redis: Redis,
        bucket_key: str,
        script_hash: str,
    ):
        self.rates = rates
        self.redis = redis
        self.bucket_key = bucket_key
        self.script_hash = script_hash
        self.failing_rate = None

    @classmethod
    def init(
        cls,
        rates: List[Rate],
        redis: Redis,
        bucket_key: str,
    ):
        script_hash = redis.script_load(LuaScript.PUT_ITEM)

        if isawaitable(script_hash):

            async def _async_init(script_hash):
                script_hash = await script_hash
                return cls(rates, redis, bucket_key, script_hash)

            return _async_init(script_hash)

        return cls(rates, redis, bucket_key, script_hash)

    def _check_and_insert(self, item: RateItem) -> Union[Rate, None, Awaitable[Optional[Rate]]]:
        keys = [self.bucket_key]

        args = [
            item.timestamp,
            item.weight,
            # NOTE: this is to avoid key collision since we are using ZSET
            f"{item.name}:{id_generator()}:",  # noqa: E231
            len(self.rates),
            *[value for rate in self.rates for value in (rate.interval, rate.limit)],
        ]

        idx = self.redis.evalsha(self.script_hash, len(keys), *keys, *args)

        def _handle_sync(returned_idx: int):
            assert isinstance(returned_idx, int), "Not int"
            if returned_idx < 0:
                return None

            return self.rates[returned_idx]

        return _handle_sync(idx)

    def put(self, item: RateItem) -> bool:
        failing_rate = self._check_and_insert(item)

        assert isinstance(failing_rate, Rate) or failing_rate is None
        self.failing_rate = failing_rate
        return not bool(self.failing_rate)

    def leak(self, current_timestamp: Optional[int] = None) -> int:
        assert current_timestamp is not None
        return self.redis.zremrangebyscore(
            self.bucket_key,
            0,
            current_timestamp - self.rates[-1].interval,
        )

    def flush(self):
        self.failing_rate = None
        return self.redis.delete(self.bucket_key)

    def count(self):
        return self.redis.zcard(self.bucket_key)

    def peek(self, index: int) -> RateItem | None:
        items = self.redis.zrange(
            self.bucket_key,
            -1 - index,
            -1 - index,
            withscores=True,
            score_cast_func=int,
        )

        if not items:
            return None

        def _handle_items(received_items: List[Tuple[str, int]]):
            if not received_items:
                return None

            item = received_items[0]
            rate_item = RateItem(name=str(item[0]), timestamp=item[1])
            return rate_item

        assert isinstance(items, list)
        return _handle_items(items)

    def close(self):
        self.redis.close()


class AsyncRedisBucket(AsyncAbstractBucket):
    """A bucket using redis for storing data
    - We are not using redis' built-in TIME since it is non-deterministic
    - In distributed context, use local server time or a remote time server
    - Each bucket instance use a dedicated connection to avoid race-condition
    - can be either sync or async
    """

    rates: List[Rate]
    failing_rate: Optional[Rate]
    bucket_key: str
    script_hash: str
    redis: AsyncRedis

    def __init__(
        self,
        rates: List[Rate],
        redis: AsyncRedis,
        bucket_key: str,
        script_hash: str,
    ):
        self.rates = rates
        self.redis = redis
        self.bucket_key = bucket_key
        self.script_hash = script_hash
        self.failing_rate = None

    @classmethod
    def init(
        cls,
        rates: List[Rate],
        redis: AsyncRedis,
        bucket_key: str,
    ):
        async def _async_init():
            script_hash = await redis.script_load(LuaScript.PUT_ITEM)
            return cls(rates, redis, bucket_key, script_hash)

        return _async_init()

    async def _check_and_insert(self, item: RateItem) -> Rate | None:
        keys = [self.bucket_key]

        args = [
            item.timestamp,
            item.weight,
            # NOTE: this is to avoid key collision since we are using ZSET
            f"{item.name}:{id_generator()}:",  # noqa: E231
            len(self.rates),
            *[value for rate in self.rates for value in (rate.interval, rate.limit)],
        ]

        idx = await self.redis.evalsha(self.script_hash, len(keys), *keys, *args)

        assert isinstance(idx, int), "Not int"
        if idx < 0:
            return None
        else:
            return self.rates[idx]

    async def put(self, item: RateItem) -> bool:
        """Add item to key"""
        failing_rate = await self._check_and_insert(item)

        self.failing_rate = failing_rate
        return not bool(self.failing_rate)

    async def leak(self, current_timestamp: Optional[int] = None) -> int:
        assert current_timestamp is not None

        return await self.redis.zremrangebyscore(
            self.bucket_key,
            0,
            current_timestamp - self.rates[-1].interval,
        )

    async def flush(self):
        self.failing_rate = None
        return await self.redis.delete(self.bucket_key)

    async def count(self):
        return await self.redis.zcard(self.bucket_key)

    async def peek(self, index: int) -> RateItem | None:
        items = await self.redis.zrange(
            self.bucket_key,
            -1 - index,
            -1 - index,
            withscores=True,
            score_cast_func=int,
        )
        if not items:
            return None
        item = items[0]
        rate_item = RateItem(name=str(item[0]), timestamp=item[1])

        return rate_item

    async def close(self):
        await self.redis.aclose()

    async def waiting(self, item: RateItem) -> int:
        wait = super().waiting(item)

        if isawaitable(wait):
            wait = await wait

        assert isinstance(wait, int)
        return wait
