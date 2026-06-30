"""Self-check for the gated tracemalloc heap-dump diagnostic.

Off by default (no env) => zero overhead, no tracing started. When enabled it
must take a snapshot and not raise. Used for one-pod, time-boxed prod profiling
to identify the live allocations driving the OOM.
"""

import tracemalloc

from s3proxy import app


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv("S3PROXY_TRACEMALLOC", raising=False)
    assert app._maybe_start_tracemalloc() is None


def test_dump_is_noop_when_not_tracing():
    # Should not raise even if tracemalloc isn't running.
    if tracemalloc.is_tracing():
        tracemalloc.stop()
    app._dump_tracemalloc()  # no exception = pass


def test_dump_reports_allocations_when_tracing():
    tracemalloc.start(2)
    try:
        blob = bytearray(4 * 1024 * 1024)  # 4MB, should show up
        # Capture warning logs to confirm it emits a snapshot + top lines.
        events = []
        import structlog

        app.logger = structlog.wrap_logger(
            app.logger, processors=[lambda _l, _m, ev: events.append(ev) or ev]
        )
        app._dump_tracemalloc(limit=5)
        assert blob is not None
        assert any(e.get("event") == "TRACEMALLOC_SNAPSHOT" for e in events)
        assert any(e.get("event") == "TRACEMALLOC_TOP" for e in events)
    finally:
        tracemalloc.stop()
