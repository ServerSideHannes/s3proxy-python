"""Unit-test fixtures."""

import pytest

import s3proxy.concurrency as concurrency


@pytest.fixture(autouse=True)
def no_backpressure_wait():
    """Reject immediately instead of waiting out the backpressure timeout.

    The rejection tests fill the budget to capacity and then try to acquire more,
    which can never succeed -- with the production timeout each such test sleeps
    the full BACKPRESSURE_TIMEOUT before asserting SlowDown. None of the in-process
    unit tests exercise wait-then-succeed backpressure, so a 0 timeout gives the
    same SlowDown with no wall-clock wait. (Patched at runtime, not via env, so it
    doesn't depend on import order and never leaks past a test.)
    """
    original = concurrency.BACKPRESSURE_TIMEOUT
    concurrency.BACKPRESSURE_TIMEOUT = 0
    yield
    concurrency.BACKPRESSURE_TIMEOUT = original
