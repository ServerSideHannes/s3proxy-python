"""Self-check: client V1 ListObjects is served via the backend's V2 API.

Hetzner Object Storage (and other modern S3 backends) only implement
ListObjectsV2 and reject legacy V1 ListObjects with HTTP 400 — which broke
every V1 client (scylla-manager's rclone 1.51.0, barman-cloud-backup-delete).

This proves handle_list_objects_v1:
- calls the backend's list_objects_v2 (never list_objects_v1),
- maps the client's V1 marker onto V2 StartAfter,
- filters internal keys, and
- synthesizes a V1 NextMarker from the largest backend key when truncated.
"""

import asyncio
import contextlib
import datetime as dt
from xml.etree.ElementTree import fromstring

from s3proxy.handlers.buckets import BucketHandlerMixin

INTERNAL_PREFIX = "s3proxy-internal/"
_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


class _URL:
    def __init__(self, path, query):
        self.path = path
        self.query = query


class _Request:
    def __init__(self, path, query):
        self.url = _URL(path, query)


class _FakeClient:
    def __init__(self, resp):
        self.resp = resp
        self.v2_calls = []

    async def list_objects_v1(self, *a, **k):  # must never be hit
        raise AssertionError("backend V1 ListObjects must not be called")

    async def list_objects_v2(
        self,
        bucket,
        prefix=None,
        continuation_token=None,
        max_keys=1000,
        delimiter=None,
        start_after=None,
    ):
        self.v2_calls.append({"start_after": start_after, "delimiter": delimiter, "prefix": prefix})
        return self.resp

    async def head_object(self, bucket, key):
        return {"Metadata": {}}


class _Handler(BucketHandlerMixin):
    handle_list_objects_v1 = BucketHandlerMixin.handle_list_objects_v1
    _process_list_objects = BucketHandlerMixin._process_list_objects

    def __init__(self, client):
        self._fake = client

    def _parse_bucket(self, path):
        return path.lstrip("/").split("/")[0]

    def _is_internal_key(self, key):
        return key.startswith(INTERNAL_PREFIX)

    def _get_plaintext_size(self, meta, fallback):
        return fallback

    def _get_effective_etag(self, meta, fallback):
        return fallback.strip('"')

    def _client(self, creds):
        client = self._fake

        @contextlib.asynccontextmanager
        async def cm():
            yield client

        return cm()


def _obj(key):
    return {
        "Key": key,
        "Size": 9,
        "ETag": '"e"',
        "LastModified": dt.datetime(2026, 6, 30, 9, 0, 0),
        "StorageClass": "STANDARD",
    }


def _run(resp, query, marker_expected_start_after):
    handler = _Handler(_FakeClient(resp))
    req = _Request("/postgres-backups", query)
    out = asyncio.run(handler.handle_list_objects_v1(req, creds=None))
    root = fromstring(out.body)
    return (
        handler,
        {c.tag.replace(_NS, ""): c.text for c in root},
        [e.find(f"{_NS}Key").text for e in root.findall(f"{_NS}Contents")],
    )


def test_v1_served_via_v2_marker_maps_to_start_after():
    resp = {
        "Contents": [_obj("a.txt"), _obj(f"{INTERNAL_PREFIX}meta"), _obj("b.txt")],
        "CommonPrefixes": [],
        "IsTruncated": True,
    }
    handler, fields, keys = _run(resp, "prefix=&marker=last-seen.txt", "last-seen.txt")

    # Backend was hit via V2 with the client marker mapped to StartAfter.
    assert handler._fake.v2_calls == [
        {"start_after": "last-seen.txt", "delimiter": None, "prefix": ""}
    ]
    # Internal key filtered out; real keys preserved in order.
    assert keys == ["a.txt", "b.txt"]
    # Truncated -> NextMarker is the largest RAW backend key (incl. would-be internal).
    assert fields["IsTruncated"] == "true"
    assert fields["NextMarker"] == f"{INTERNAL_PREFIX}meta"  # max("a.txt","b.txt",internal)
    # Echoes the request marker.
    assert fields["Marker"] == "last-seen.txt"


def test_v1_not_truncated_has_no_next_marker():
    resp = {"Contents": [_obj("only.txt")], "CommonPrefixes": [], "IsTruncated": False}
    _, fields, keys = _run(resp, "prefix=", None)
    assert keys == ["only.txt"]
    assert fields["IsTruncated"] == "false"
    assert fields.get("NextMarker") is None


if __name__ == "__main__":
    test_v1_served_via_v2_marker_maps_to_start_after()
    test_v1_not_truncated_has_no_next_marker()
    print("ok")
