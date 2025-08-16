# tests/test_notifier_limiter.py
import asyncio, time, threading, pytest
from pyrate_limiter import Rate, InMemoryBucket
from pyrate_limiter.notifierlimiter import NotifierLimiter  # adjust import path

@pytest.mark.asyncio
async def test_constructor_and_buckets():
    rl = NotifierLimiter([Rate(2, 1000)])
    assert [(r.limit, r.interval) for r in rl.buckets()[0].rates] == [(2, 1000)]

@pytest.mark.asyncio
async def test_weight_zero_fastpath():
    rl = NotifierLimiter([Rate(1, 1000)])
    t0 = time.perf_counter()
    ok = await rl.try_acquire_async("x", weight=0)
    dt = (time.perf_counter()-t0)*1000
    assert ok and dt < 10

@pytest.mark.asyncio
async def test_nonblocking_fail_then_blocking_succeeds():
    rl = NotifierLimiter([Rate(1, 1000)])
    assert await rl.try_acquire_async("x")
    t0 = time.perf_counter()
    ok = await rl.try_acquire_async("x", blocking=False)
    dt = (time.perf_counter()-t0)*1000
    assert (not ok) and dt < 20
    t0 = time.perf_counter()
    ok = await rl.try_acquire_async("x", blocking=True)
    dt = (time.perf_counter()-t0)*1000
    assert ok and 900 <= dt <= 1300

@pytest.mark.asyncio
async def test_timeout_expires():
    rl = NotifierLimiter([Rate(1, 2000)])
    assert await rl.try_acquire_async("x")
    t0 = time.perf_counter()
    ok = await rl.try_acquire_async("x", blocking=True, timeout=0.3)
    dt = (time.perf_counter()-t0)
    assert (not ok) and 0.25 <= dt <= 0.5

@pytest.mark.asyncio
async def test_multiple_async_waiters_wake_on_capacity():
    rl = NotifierLimiter([Rate(1, 1000)])
    assert await rl.try_acquire_async("x")
    results = []
    async def waiter(i): 
        results.append((i, await rl.try_acquire_async(f"x{i}")))
    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.gather(*tasks)
    assert sum(1 for _, ok in results if ok) == 3  # - all eventually True

def test_sync_nonblocking_and_blocking_paths():
    rl = NotifierLimiter([Rate(1, 1000)])
    assert rl.try_acquire("x")
    t0 = time.perf_counter()
    ok = rl.try_acquire("x", blocking=False)
    dt = (time.perf_counter()-t0)*1000
    assert (not ok) and dt < 10
    t0 = time.perf_counter(); ok = rl.try_acquire("x", blocking=True)
    dt = (time.perf_counter()-t0)*1000
    assert ok and 900 <= dt <= 1300

def test_sync_timeout():
    rl = NotifierLimiter([Rate(1, 2000)])
    assert rl.try_acquire("x")
    t0 = time.perf_counter()
    ok = rl.try_acquire("x", blocking=True, timeout=0.3)
    dt = (time.perf_counter()-t0)
    assert (not ok) and 0.25 <= dt <= 0.5

@pytest.mark.asyncio
async def test_async_and_sync_interleave_and_notify_each_other():
    rl = NotifierLimiter([Rate(1, 1000)])
    assert rl.try_acquire("x")
    sync_done = threading.Event()
    def sync_waiter():
        ok = rl.try_acquire("s")
        sync_done.ok = ok
        sync_done.set()
    th = threading.Thread(target=sync_waiter)
    th.start()
    await asyncio.sleep(0.05)
    ok_async = await rl.try_acquire_async("a")  # should wake sync via notify_all on put
    th.join(timeout=2)
    assert ok_async and getattr(sync_done, "ok", False)


@pytest.mark.asyncio
async def test_per_loop_events_isolated():
    rl = NotifierLimiter([Rate(1, 1000)])
    assert await rl.try_acquire_async("x")  # fill once

    res = {"ok": None}
    def worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            res["ok"] = loop.run_until_complete(rl.try_acquire_async("y"))
        finally:
            loop.close()

    t = threading.Thread(target=worker)
    t.start()
    await asyncio.sleep(1.1)   # capacity frees
    t.join(timeout=2)

    assert res["ok"] is True

@pytest.mark.asyncio
async def test_decorator_sync_and_async():
    rl = NotifierLimiter([Rate(2, 1000)])
    count = {"v": 0}
    @rl.as_decorator(name="d", weight=1)
    def inc(n): 
        count["v"] += n
    @rl.as_decorator(name="d", weight=1)
    async def ainc(n): 
        count["v"] += n
    inc(1)
    await ainc(1)
    assert count["v"] == 2

@pytest.mark.asyncio
async def test_concurrency_fair_progress():
    rl = NotifierLimiter([Rate(5, 1000)])
    started, finished = [], []
    async def worker(i):
        started.append(i)
        ok = await rl.try_acquire_async(f"k{i}")
        if ok: 
            finished.append(i)
    await asyncio.gather(*(worker(i) for i in range(10)))
    assert len(finished) == 10 and set(finished) == set(range(10))

def test_close_no_exceptions():
    rl = NotifierLimiter([Rate(1, 1000)])
    rl.close()


@pytest.mark.asyncio
async def test_multiple_rates_short_windows():
    rl = NotifierLimiter([Rate(1, 100), Rate(3, 300)])
    assert await rl.try_acquire_async("x")          # t=0ms
    assert not await rl.try_acquire_async("x", blocking=False)
    ok = await rl.try_acquire_async("x")            # waits ~≤100ms
    assert ok

@pytest.mark.asyncio
async def test_weighted_acquire():
    rl = NotifierLimiter([Rate(2, 200)])
    assert await rl.try_acquire_async("w", weight=2)
    assert not await rl.try_acquire_async("w", weight=1, blocking=False)

def test_nonblocking_with_timeout_raises():
    rl = NotifierLimiter([Rate(1, 1000)])
    with pytest.raises(RuntimeError):
        rl.try_acquire("x", blocking=False, timeout=0.1)

@pytest.mark.asyncio
async def test_sync_blocks_after_async():
    rl = NotifierLimiter([Rate(1, 200)])
    assert await rl.try_acquire_async("x")

    t0 = time.perf_counter()
    ok = await asyncio.get_running_loop().run_in_executor(None, rl.try_acquire, "y")
    dt = (time.perf_counter() - t0) * 1000

    assert ok
    assert 150 <= dt <= 400

@pytest.mark.asyncio
async def test_two_async_waiters_progress():
    rl = NotifierLimiter([Rate(1, 200)])
    assert await rl.try_acquire_async("seed")

    t0 = time.perf_counter()
    a = asyncio.create_task(rl.try_acquire_async("a"))
    b = asyncio.create_task(rl.try_acquire_async("b"))

    assert await a
    assert await b

    dt = (time.perf_counter() - t0) * 1000
    assert 350 <= dt <= 700  # ~2 slots at ~200ms

def test_decorator_blocks_then_runs_sync():
    rl = NotifierLimiter([Rate(1, 200)])
    hits = {"n": 0}

    @rl.as_decorator(name="d", weight=1)
    def inc():
        hits["n"] += 1

    inc()

    t0 = time.perf_counter()
    inc()
    dt = (time.perf_counter() - t0) * 1000

    assert hits["n"] == 2
    assert 150 <= dt <= 400

@pytest.mark.asyncio
async def test_decorator_blocks_then_runs_async():
    rl = NotifierLimiter([Rate(1, 200)])
    hits = {"n": 0}

    @rl.as_decorator(name="d2", weight=1)
    async def inc():
        hits["n"] += 1

    await inc()

    t0 = time.perf_counter()
    await inc()
    dt = (time.perf_counter() - t0) * 1000

    assert hits["n"] == 2
    assert 150 <= dt <= 400

@pytest.mark.asyncio
async def test_buffer_ms_effect_small():
    rl = NotifierLimiter([Rate(1, 120)], buffer_ms=0)
    assert await rl.try_acquire_async("x")

    t0 = time.perf_counter()
    ok = await rl.try_acquire_async("x")
    dt = (time.perf_counter() - t0) * 1000

    assert ok
    assert 90 <= dt <= 200

def test_close_idempotent():
    rl = NotifierLimiter([Rate(1, 100)])
    rl.close()
    rl.close()  # no exception

@pytest.mark.asyncio
async def test_weight_zero_async_nonblocking():
    rl = NotifierLimiter([Rate(1, 1000)])

    t0 = time.perf_counter()
    ok = await rl.try_acquire_async("x", weight=0, blocking=False)
    dt = (time.perf_counter() - t0) * 1000

    assert ok
    assert dt < 10
