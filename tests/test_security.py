import pytest

from sockethive.security import RateLimiter, token_matches


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_token_matches():
    assert token_matches("s3cret", "s3cret")
    assert not token_matches("s3cret", "s3cre")
    assert not token_matches("s3cret", None)
    assert not token_matches("s3cret", 123)


def test_rate_limiter_burst_then_refill():
    clock = FakeClock()
    limiter = RateLimiter(rate=2, burst=3, clock=clock)
    assert [limiter.acquire("a") for _ in range(3)] == [0.0, 0.0, 0.0]
    assert limiter.acquire("a") == pytest.approx(0.5)  # 1 token at 2/s
    clock.now += 0.5
    assert limiter.acquire("a") == 0.0
    assert limiter.acquire("a") > 0


def test_rate_limiter_refill_is_capped_at_burst():
    clock = FakeClock()
    limiter = RateLimiter(rate=10, burst=2, clock=clock)
    limiter.acquire("a")
    clock.now += 60
    assert [limiter.acquire("a") for _ in range(2)] == [0.0, 0.0]
    assert limiter.acquire("a") > 0


def test_rate_limiter_keys_are_independent():
    limiter = RateLimiter(rate=1, burst=1, clock=FakeClock())
    assert limiter.acquire("10.0.0.1") == 0.0
    assert limiter.acquire("10.0.0.1") > 0
    assert limiter.acquire("10.0.0.2") == 0.0


def test_rate_limiter_evicts_least_recently_used():
    limiter = RateLimiter(rate=1, burst=1, max_keys=2, clock=FakeClock())
    limiter.acquire("a")
    limiter.acquire("b")
    limiter.acquire("c")  # evicts "a"
    assert len(limiter._buckets) == 2
    assert limiter.acquire("a") == 0.0  # fresh bucket again


@pytest.mark.parametrize(("rate", "burst"), [(0, 1), (-1, 1), (1, 0)])
def test_rate_limiter_rejects_bad_settings(rate, burst):
    with pytest.raises(ValueError):
        RateLimiter(rate, burst)
