import threading

import pytest

from sockethive.dedup import ResponseCache


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_first_claim_owns_and_repeats_get_stored_response():
    cache = ResponseCache(ttl=60, max_entries=10)
    future, owner = cache.claim("k")
    assert owner
    cache.complete(future, {"ok": True, "result": 1})
    again, owner = cache.claim("k")
    assert not owner
    assert again.result(timeout=0) == {"ok": True, "result": 1}


def test_duplicate_waits_for_in_flight_original():
    cache = ResponseCache(ttl=60, max_entries=10)
    future, _ = cache.claim("k")
    threading.Timer(0.1, cache.complete, args=(future, {"result": "done"})).start()
    duplicate, owner = cache.claim("k")
    assert not owner
    assert duplicate.result(timeout=2) == {"result": "done"}


def test_entries_expire_after_ttl():
    clock = FakeClock()
    cache = ResponseCache(ttl=10, max_entries=10, clock=clock)
    future, _ = cache.claim("k")
    cache.complete(future, {})
    clock.now = 9.9
    assert cache.claim("k")[1] is False
    clock.now = 10.0
    assert cache.claim("k")[1] is True


def test_oldest_entries_are_evicted_beyond_capacity():
    cache = ResponseCache(ttl=60, max_entries=2)
    for key in "abc":
        cache.claim(key)
    assert len(cache) == 2
    assert cache.claim("a")[1] is True  # evicted, so it runs again


def test_release_forgets_key_but_wakes_waiters():
    cache = ResponseCache(ttl=60, max_entries=10)
    future, _ = cache.claim("k")
    waiter, _ = cache.claim("k")
    cache.release("k", future, {"error": "rate limited"})
    assert waiter.result(timeout=0) == {"error": "rate limited"}
    assert cache.claim("k")[1] is True


@pytest.mark.parametrize(("ttl", "size"), [(0, 1), (1, 0)])
def test_rejects_bad_settings(ttl, size):
    with pytest.raises(ValueError):
        ResponseCache(ttl, size)
