"""LIST must report plaintext size/etag for multipart-encrypted objects.

Multipart uploads can't stamp ``plaintext-size`` user metadata on the backend
object (user metadata is fixed at CreateMultipartUpload, before the final size
is known) — their plaintext size lives in the ``.meta`` sidecar. If
``_process_list_objects`` falls back to the raw backend Size it reports the
ciphertext size, and sync clients (scylla-manager's rclone) see "sizes differ"
and re-upload every multipart object on every pass.

These tests pin:
- sidecar resolution for multipart objects (size + synthetic etag, matching
  what HEAD and CompleteMultipartUpload report),
- raw-size fallback when there is no sidecar (legacy/unencrypted object),
- user-metadata resolution still winning for simple PUT objects,
- the attr cache eliminating repeat backend round-trips, scoped to the
  backend ETag so overwrites re-resolve,
- CompleteMultipartUpload priming the cache.
"""

import asyncio
import datetime as dt
from urllib.parse import urlencode

import pytest

from s3proxy import crypto
from s3proxy.handlers.buckets import BucketHandlerMixin
from s3proxy.state import (
    MultipartMetadata,
    encode_multipart_metadata,
    plaintext_attr_cache,
    synthetic_multipart_etag,
)
from s3proxy.state.attr_cache import PlaintextAttrCache
from s3proxy.state.metadata import _internal_meta_key, persist_upload_state
from tests.conftest import MockS3Response as _Body

INTERNAL_PREFIX = ".s3proxy-internal/"


@pytest.fixture(autouse=True)
def fresh_cache(monkeypatch):
    """Isolate each test from the process-global cache singleton."""
    cache = PlaintextAttrCache()
    monkeypatch.setattr("s3proxy.handlers.buckets.plaintext_attr_cache", cache)
    plaintext_attr_cache.clear()
    yield cache
    plaintext_attr_cache.clear()


class FakeHandler:
    _resolve_object = BucketHandlerMixin._resolve_object
    _process_list_objects = BucketHandlerMixin._process_list_objects
    _list_entry = staticmethod(BucketHandlerMixin._list_entry)

    def _is_internal_key(self, key):
        return key.startswith(INTERNAL_PREFIX)

    def _get_plaintext_size(self, meta, fallback):
        return int(meta.get("plaintext-size", fallback))

    def _get_effective_etag(self, meta, fallback):
        return meta.get("client-etag", fallback.strip('"'))


class FakeClient:
    """Backend with per-key user metadata and .meta sidecars."""

    def __init__(self, metadata=None, sidecars=None, fail_head_key=None):
        self.metadata = metadata or {}
        self.sidecars = sidecars or {}
        self.fail_head_key = fail_head_key
        self.head_calls = 0
        self.get_calls = 0

    async def head_object(self, bucket, key):
        self.head_calls += 1
        if key == self.fail_head_key:
            raise RuntimeError("backend HEAD failed")
        return {"Metadata": self.metadata.get(key, {})}

    async def get_object(self, bucket, key):
        self.get_calls += 1
        if key in self.sidecars:
            encoded = encode_multipart_metadata(self.sidecars[key])
            return {"Body": _Body(encoded.encode())}
        from botocore.exceptions import ClientError

        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")


def _obj(key, size, etag="backend-etag"):
    return {
        "Key": key,
        "Size": size,
        "ETag": f'"{etag}"',
        "LastModified": dt.datetime(2026, 7, 12, 9, 0, 0),
        "StorageClass": "STANDARD",
    }


def _mp_meta(plaintext_size):
    return MultipartMetadata(
        version=2, part_count=3, total_plaintext_size=plaintext_size, wrapped_dek=b"x" * 40
    )


def test_multipart_size_resolved_from_sidecar():
    handler = FakeHandler()
    client = FakeClient(sidecars={_internal_meta_key("mp.db"): _mp_meta(12345)})

    result = asyncio.run(
        handler._process_list_objects(client, "bucket", [_obj("mp.db", size=99999)])
    )

    assert result[0]["size"] == 12345
    assert result[0]["etag"] == synthetic_multipart_etag(12345)


def test_no_sidecar_falls_back_to_listed_size():
    handler = FakeHandler()
    client = FakeClient()

    result = asyncio.run(
        handler._process_list_objects(client, "bucket", [_obj("legacy.bin", size=777)])
    )

    assert result[0]["size"] == 777
    assert result[0]["etag"] == "backend-etag"


def test_simple_put_metadata_wins_without_sidecar_lookup():
    handler = FakeHandler()
    client = FakeClient(
        metadata={"small.txt": {"plaintext-size": "42", "client-etag": "abc"}},
    )

    result = asyncio.run(
        handler._process_list_objects(client, "bucket", [_obj("small.txt", size=100)])
    )

    assert result[0]["size"] == 42
    assert result[0]["etag"] == "abc"
    assert client.get_calls == 0


def test_repeat_listing_served_from_cache():
    handler = FakeHandler()
    client = FakeClient(
        metadata={"small.txt": {"plaintext-size": "42", "client-etag": "abc"}},
        sidecars={_internal_meta_key("mp.db"): _mp_meta(12345)},
    )
    contents = [_obj("mp.db", size=99999), _obj("small.txt", size=100)]

    first = asyncio.run(handler._process_list_objects(client, "bucket", contents))
    head_calls, get_calls = client.head_calls, client.get_calls
    second = asyncio.run(handler._process_list_objects(client, "bucket", contents))

    assert first == second
    assert client.head_calls == head_calls
    assert client.get_calls == get_calls


def test_cache_scoped_to_backend_etag():
    handler = FakeHandler()
    client = FakeClient(sidecars={_internal_meta_key("mp.db"): _mp_meta(12345)})

    asyncio.run(handler._process_list_objects(client, "bucket", [_obj("mp.db", 99999, "v1")]))
    head_calls = client.head_calls
    # Overwritten object -> new backend ETag -> must re-resolve, not reuse.
    asyncio.run(handler._process_list_objects(client, "bucket", [_obj("mp.db", 88888, "v2")]))

    assert client.head_calls == head_calls + 1


def test_failed_head_propagates_and_does_not_cache(fresh_cache):
    handler = FakeHandler()
    client = FakeClient(fail_head_key="broken.db")
    with pytest.raises(RuntimeError, match="backend HEAD failed"):
        asyncio.run(handler._process_list_objects(client, "bucket", [_obj("broken.db", size=555)]))
    assert len(fresh_cache) == 0


@pytest.mark.asyncio
async def test_concurrent_listing_coalesces_metadata_lookups():
    handler = FakeHandler()
    client = FakeClient(metadata={"small.txt": {"plaintext-size": "42", "client-etag": "abc"}})
    original = client.head_object

    async def delayed(*args):
        await asyncio.sleep(0.01)
        return await original(*args)

    client.head_object = delayed
    results = await asyncio.gather(
        *[
            handler._process_list_objects(client, "bucket", [_obj("small.txt", size=100)])
            for _ in range(8)
        ]
    )
    assert all(r[0]["size"] == 42 for r in results)
    assert client.head_calls == 1


def test_cache_evicts_least_recently_used():
    cache = PlaintextAttrCache(maxsize=2)
    cache.put("b", "k1", "e1", 1, "t1")
    cache.put("b", "k2", "e2", 2, "t2")
    assert cache.get("b", "k1", "e1") == (1, "t1")  # refresh k1
    cache.put("b", "k3", "e3", 3, "t3")  # evicts k2

    assert cache.get("b", "k2", "e2") is None
    assert cache.get("b", "k1", "e1") == (1, "t1")
    assert cache.get("b", "k3", "e3") == (3, "t3")


def test_cache_ignores_empty_backend_etag():
    cache = PlaintextAttrCache()
    cache.put("b", "k", "", 1, "t")
    assert cache.get("b", "k", "") is None
    assert len(cache) == 0


class _FakeURL:
    def __init__(self, path, query):
        self.path = path
        self.query = query


class _FakeRequest:
    def __init__(self, path, query, body):
        self.url = _FakeURL(path, query)
        self.headers = {}
        self._body = body

    async def body(self):
        return self._body


class _ClientCM:
    def __init__(self, client):
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *exc):
        return False


async def test_complete_multipart_primes_cache(
    handler, mock_s3, mock_s3_client, settings, credentials
):
    bucket, key = "test-bucket", "backups/primed.bin"
    kid, kek = settings.keyring.key_for(credentials.access_key)
    dek = crypto.generate_dek()
    plaintext = b"payload" * 128

    resp = await mock_s3.create_multipart_upload(bucket, key)
    upload_id = resp["UploadId"]
    ciphertext = crypto.encrypt(plaintext, dek, crypto.derive_part_nonce(upload_id, 1))
    await mock_s3.upload_part(bucket, key, upload_id, 1, ciphertext)
    await persist_upload_state(
        mock_s3_client, bucket, key, upload_id, crypto.wrap_key(dek, kek), kid
    )
    handler._client = lambda creds: _ClientCM(mock_s3_client)

    body = (
        b"<CompleteMultipartUpload>"
        b"<Part><PartNumber>1</PartNumber><ETag>&quot;etag-1&quot;</ETag></Part>"
        b"</CompleteMultipartUpload>"
    )
    resp = await handler.handle_complete_multipart_upload(
        _FakeRequest(f"/{bucket}/{key}", urlencode({"uploadId": upload_id}), body),
        credentials,
    )
    assert resp.status_code == 200

    backend_etag = str(mock_s3.objects[f"{bucket}/{key}"]["ETag"]).strip('"')
    assert plaintext_attr_cache.get(bucket, key, backend_etag) == (
        len(plaintext),
        synthetic_multipart_etag(len(plaintext)),
    )
