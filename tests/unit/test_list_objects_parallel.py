"""Self-check for the parallel HEAD fan-out in BucketHandlerMixin._process_list_objects.

Sequential HEADs on a recursive list stack into a client-timeout-tripping stall.
This proves: HEADs run concurrently, output order matches input order, internal
keys are skipped, and a failing HEAD falls back to the listed size/etag.

The repo's package __init__ chain currently pulls in modules with pre-existing
Py2 `except A, B:` syntax (utils.py, dashboard/*) that won't import under Py3,
so we load buckets.py directly with stubbed siblings to exercise the real code.
"""

import asyncio
import datetime as dt
import importlib.util
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _load_buckets():
    def stub(name, **attrs):
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
        return m

    stub("s3proxy")
    stub("s3proxy.handlers")
    stub("s3proxy.xml_responses")
    stub("s3proxy.client", S3Credentials=object)
    stub("s3proxy.errors", S3Error=type("S3Error", (Exception,), {}))
    stub(
        "s3proxy.state",
        INTERNAL_PREFIX="s3proxy-internal/",
        META_SUFFIX_LEGACY=".s3proxy-meta",
        delete_multipart_metadata=lambda *a, **k: None,
    )
    stub("s3proxy.xml_utils", find_element=lambda *a, **k: None, find_elements=lambda *a, **k: [])
    stub("s3proxy.handlers.base", BaseHandler=object)

    spec = importlib.util.spec_from_file_location(
        "s3proxy.handlers.buckets", REPO / "s3proxy" / "handlers" / "buckets.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["s3proxy.handlers.buckets"] = mod
    spec.loader.exec_module(mod)
    return mod


buckets = _load_buckets()
INTERNAL_PREFIX = "s3proxy-internal/"


class FakeHandler:
    _process_list_objects = buckets.BucketHandlerMixin._process_list_objects

    def __init__(self):
        self.inflight = 0
        self.max_inflight = 0

    def _is_internal_key(self, key):
        return key.startswith(INTERNAL_PREFIX)

    def _get_plaintext_size(self, meta, fallback):
        return int(meta.get("plaintext-size", fallback))

    def _get_effective_etag(self, meta, fallback):
        return meta.get("client-etag", fallback.strip('"'))


class FakeClient:
    def __init__(self, handler, fail_key=None):
        self.handler = handler
        self.fail_key = fail_key

    async def head_object(self, bucket, key):
        self.handler.inflight += 1
        self.handler.max_inflight = max(self.handler.max_inflight, self.handler.inflight)
        try:
            await asyncio.sleep(0.02)  # simulate backend round-trip
            if key == self.fail_key:
                raise RuntimeError("backend HEAD failed")
            return {"Metadata": {"plaintext-size": "111", "client-etag": f"etag-{key}"}}
        finally:
            self.handler.inflight -= 1


def _obj(key, size=999):
    return {
        "Key": key,
        "Size": size,
        "ETag": '"raw-etag"',
        "LastModified": dt.datetime(2026, 6, 24, 9, 0, 0),
        "StorageClass": "STANDARD",
    }


def test_parallel_order_and_fallback():
    handler = FakeHandler()
    client = FakeClient(handler, fail_key="b")
    contents = [
        _obj("a"),
        _obj(f"{INTERNAL_PREFIX}skip-me"),  # internal -> dropped
        _obj("b", size=42),                  # HEAD fails -> fallback to listed size/etag
        _obj("c"),
    ]

    result = asyncio.run(handler._process_list_objects(client, "bucket", contents))

    # Internal key dropped, order preserved.
    assert [o["key"] for o in result] == ["a", "b", "c"]
    # Successful HEAD -> plaintext size + client-etag.
    assert result[0]["size"] == 111
    assert result[0]["etag"] == "etag-a"
    # Failed HEAD -> fallback to listed size + stripped raw etag.
    assert result[1]["size"] == 42
    assert result[1]["etag"] == "raw-etag"
    # HEADs actually ran concurrently (would be 1 if sequential), and stayed bounded.
    assert handler.max_inflight > 1
    assert handler.max_inflight <= buckets.LIST_HEAD_CONCURRENCY


if __name__ == "__main__":
    test_parallel_order_and_fallback()
    print("ok")
