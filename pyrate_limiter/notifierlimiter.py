import asyncio
import logging
import time
from functools import wraps
from inspect import isawaitable
from threading import Condition, RLock, local
from typing import Any, Callable, Iterable, List, Union
from weakref import WeakKeyDictionary

from .abstracts import AbstractBucket, BucketFactory, Rate
from .buckets import InMemoryBucket
from .limiter import SingleBucketFactory, combined_lock

logger = logging.getLogger(__name__)


class NotifierLimiter:
    """
    Limiter that avoids sleep-loops:
      - sync waits via threading.Condition.wait(timeout)
      - async waits via asyncio.Event per running loop
    On each capacity change (successful put), we notify both paths.
    """

    bucket_factory: BucketFactory
    lock: Union[RLock, Iterable]
    buffer_ms: int

    _thread_local: local
    _cond: Condition
    _loop_events: "WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Event]"

    def __init__(
        self,
        argument: Union[BucketFactory, AbstractBucket, Rate, List[Rate]],
        buffer_ms: int = 0,
    ):
        self.buffer_ms = buffer_ms
        self.bucket_factory = self._init_bucket_factory(argument)
        self.lock = RLock()
        self._thread_local = local()
        self._cond = Condition(RLock())
        self._loop_events = WeakKeyDictionary()

        if isinstance(argument, AbstractBucket):
            limiter_lock = argument.limiter_lock()
            if limiter_lock is not None:
                self.lock = (limiter_lock, self.lock)

    def _init_bucket_factory(
        self,
        argument: Union[BucketFactory, AbstractBucket, Rate, List[Rate]],
    ) -> BucketFactory:
        if isinstance(argument, Rate):
            argument = [argument]
        if isinstance(argument, list):
            assert argument and isinstance(argument[0], Rate), "Invalid rates list"
            logger.info("Initializing default bucket(InMemoryBucket) with rates: %s", argument)
            argument = InMemoryBucket(argument)
        if isinstance(argument, AbstractBucket):
            argument = SingleBucketFactory(argument)
        assert isinstance(argument, BucketFactory), "Not a valid bucket/bucket-factory"
        return argument

    def buckets(self) -> List[AbstractBucket]:
        return self.bucket_factory.get_buckets()

    def dispose(self, bucket: Union[int, AbstractBucket]) -> bool:
        return self.bucket_factory.dispose(bucket)

    def _notify_all(self) -> None:
        # wake sync waiters
        with self._cond:
            self._cond.notify_all()
        # wake async waiters (per loop)
        dead = []
        for loop, ev in list(self._loop_events.items()):
            if loop.is_closed():
                dead.append(loop)
                continue
            try:
                if loop.is_running():
                    loop.call_soon_threadsafe(ev.set)
            except RuntimeError:
                dead.append(loop)
        for loop in dead:
            self._loop_events.pop(loop, None)

    def _get_async_event(self) -> asyncio.Event:
        loop = asyncio.get_running_loop()
        ev = self._loop_events.get(loop)
        if ev is None:
            ev = asyncio.Event()
            self._loop_events[loop] = ev
        return ev

    def _get_async_lock(self):
        loop = asyncio.get_running_loop()
        try:
            lock = self._thread_local.async_lock
            if self._thread_local.async_lock_loop is loop:
                return lock
        except AttributeError:
            pass
        lock = asyncio.Lock()
        self._thread_local.async_lock = lock
        self._thread_local.async_lock_loop = loop
        return lock

    def _put_now(self, name: str, weight: int) -> bool:
        # Re-wrap each attempt to get a fresh timestamp from the bucket's clock
        item = self.bucket_factory.wrap_item(name, weight)

        if isawaitable(item):
            # sync path should not hit this; caller guards
            raise RuntimeError("Unexpected awaitable RateItem in sync path")

        bucket = self.bucket_factory.get(item)
        assert isinstance(bucket, AbstractBucket)

        ok = bucket.put(item)  # type: ignore[return-value]
        assert isinstance(ok, bool)

        if ok:
            self._notify_all()

        return ok

    async def _put_now_async(self, name: str, weight: int) -> bool:
        item = self.bucket_factory.wrap_item(name, weight)
        if isawaitable(item):
            item = await item

        bucket = self.bucket_factory.get(item)

        if isawaitable(bucket):
            bucket = await bucket

        assert isinstance(bucket, AbstractBucket)
        ok = bucket.put(item)

        if isawaitable(ok):
            ok = await ok

        if ok:
            self._notify_all()

        return ok

    def _next_delay_seconds(self, name: str, weight: int) -> float:
        """
        Compute the time (in seconds) the caller should wait before the next
        acquisition attempt for the given item.

        This method:
        • Wraps the (name, weight) into a RateItem with the current timestamp.
        • Queries the underlying bucket for how many milliseconds must elapse
            before the item can be accepted.
        • Adds `self.buffer_ms` as a safety margin to avoid clock drift or
            race conditions.
        • Converts the total delay to seconds and returns it as a float.
        """
        item = self.bucket_factory.wrap_item(name, weight)
        assert not isawaitable(item)

        bucket = self.bucket_factory.get(item)
        assert isinstance(bucket, AbstractBucket)

        d = bucket.waiting(item)

        if isawaitable(d):
            raise RuntimeError("sync path should never return awaitable")

        d = int(d)
        if d < 0:
            d = 0

        return (d + self.buffer_ms) / 1000.0

    async def _next_delay_seconds_async(self, name: str, weight: int) -> float:
        item = self.bucket_factory.wrap_item(name, weight)
        if isawaitable(item):
            item = await item

        bucket = self.bucket_factory.get(item)
        if isawaitable(bucket):
            bucket = await bucket

        d = bucket.waiting(item)
        if isawaitable(d):
            d = await d

        d = int(d)
        if d < 0:
            d = 0

        return (d + self.buffer_ms) / 1000.0

    def try_acquire(self, name: str = __name__, weight: int = 1, blocking: bool = True, timeout: float = -1) -> bool:
        if weight == 0:
            return True

        if not blocking and timeout != -1:
            raise RuntimeError("Can't set timeout with non-blocking")

        with combined_lock(self.lock, blocking=True):
            if self._put_now(name, weight):
                return True
            if not blocking:
                return False

            # wait path
            deadline = (
                None
                if timeout == -1
                else asyncio.get_running_loop().time() + timeout
                if asyncio.get_event_loop_policy().get_event_loop().is_running()
                else None
            )  # best-effort if called inside loop
            # Use threading.Condition with timeout based on bucket.waiting()
            deadline = None if timeout == -1 else time.monotonic() + timeout
            while True:
                wait_s = self._next_delay_seconds(name, weight)
                if deadline is not None:
                    rem = deadline - time.monotonic()
                    if rem <= 0:
                        return False
                    wait_s = min(wait_s, rem)
                with self._cond:
                    self._cond.wait(timeout=wait_s)
                if self._put_now(name, weight):
                    return True

    async def try_acquire_async(self, name: str = __name__, weight: int = 1, blocking: bool = True, timeout: float = -1) -> bool:
        if weight == 0:
            return True
        if not blocking and timeout != -1:
            raise RuntimeError("Can't set timeout with non-blocking")

        lock = self._get_async_lock()

        async with lock:
            # fast path
            if await self._put_now_async(name, weight):
                return True
            if not blocking:
                return False

            ev = self._get_async_event()

            while True:
                wait_s = await self._next_delay_seconds_async(name, weight)
                ev.clear()
                t0 = asyncio.get_event_loop().time()
                try:
                    if timeout == -1:
                        await asyncio.wait_for(ev.wait(), timeout=wait_s)
                    else:
                        await asyncio.wait_for(ev.wait(), timeout=min(wait_s, timeout))
                except asyncio.TimeoutError:
                    pass
                dt = asyncio.get_event_loop().time() - t0
                if timeout != -1:
                    timeout = max(0.0, timeout - dt)
                if await self._put_now_async(name, weight):
                    return True
                if timeout == 0:
                    return False

    def as_decorator(self, *, name="ratelimiter", weight=1):
        def deco(func: Callable[..., Any]) -> Callable[..., Any]:
            if asyncio.iscoroutinefunction(func):

                @wraps(func)
                async def wrapper(*args, **kwargs):
                    ok = await self.try_acquire_async(name=name, weight=weight)
                    if not ok:
                        return None
                    return await func(*args, **kwargs)

                return wrapper
            else:

                @wraps(func)
                def wrapper(*args, **kwargs):
                    ok = self.try_acquire(name=name, weight=weight)
                    if not ok:
                        return None
                    return func(*args, **kwargs)

                return wrapper

        return deco

    def close(self) -> None:
        self.bucket_factory.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
