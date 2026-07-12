"""Self-check for the parallel HEAD fan-out in BucketHandlerMixin._process_list_objects.

Sequential HEADs on a recursive list stack into a client-timeout-tripping stall.
This proves: HEADs run concurrently, output order matches input order, internal
keys are skipped, and a failing HEAD falls back to the listed size/etag.
"""

import asyncio
import datetime as dt

from s3proxy.handlers.buckets import LIST_HEAD_CONCURRENCY, BucketHandlerMixin

INTERNAL_PREFIX = "s3proxy-internal/"


class FakeHandler:
    _process_list_objects = BucketHandlerMixin._process_list_objects
    _list_entry = staticmethod(BucketHandlerMixin._list_entry)

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
        _obj("b", size=42),  # HEAD fails -> fallback to listed size/etag
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
    assert handler.max_inflight <= LIST_HEAD_CONCURRENCY


if __name__ == "__main__":
    test_parallel_order_and_fallback()
    print("ok")
