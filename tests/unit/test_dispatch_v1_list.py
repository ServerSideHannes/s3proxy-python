"""Self-check: a V1 ListObjects GET routes to the list handler, not forward_request.

V1 ListObjects is `GET /bucket?prefix&delimiter&max-keys&encoding-type` with no
`list-type=2`. _dispatch_bucket used to raw-forward any bucket GET whose query
lacked list-type/delete/uploads/location, so V1 lists were sent verbatim to the
backend — which Hetzner rejects with HTTP 400. They must instead reach
handle_list_objects_v1 (which serves them via the backend's V2 API).
"""

import asyncio

from s3proxy.routing.dispatcher import RequestDispatcher


class _URL:
    def __init__(self, path, query):
        self.path = path
        self.query = query


class _Req:
    def __init__(self, method, path, query):
        self.method = method
        self.url = _URL(path, query)
        self.headers = {}


def _record(name):
    async def f(self, request, creds):
        self.called = name
        return name

    return f


class _RecordingHandler:
    def __init__(self):
        self.called = None

    handle_list_objects_v1 = _record("v1")
    handle_list_objects = _record("v2")
    forward_request = _record("forward")
    handle_get_bucket_location = _record("location")
    handle_get_bucket_versioning = _record("versioning")
    handle_list_buckets = _record("list_buckets")


def _route(method, path, query):
    handler = _RecordingHandler()
    asyncio.run(RequestDispatcher(handler).dispatch(_Req(method, path, query), creds=None))
    return handler.called


def test_v1_list_with_params_routes_to_v1_handler():
    # exactly what scylla-manager's rclone / barman-cloud-backup-delete send
    assert (
        _route("GET", "/bkt", "prefix=x%2F&delimiter=%2F&max-keys=1000&encoding-type=url") == "v1"
    )
    assert _route("GET", "/bkt", "prefix=x&marker=y") == "v1"


def test_bare_bucket_get_is_v1_list():
    assert _route("GET", "/bkt", "") == "v1"


def test_v2_list_still_routes_to_v2():
    assert _route("GET", "/bkt", "list-type=2&prefix=x") == "v2"


def test_subresource_get_still_forwards():
    assert _route("GET", "/bkt", "acl") == "forward"
    assert _route("GET", "/bkt", "versioning") == "versioning"
    assert _route("GET", "/bkt", "location") == "location"


def test_prefix_containing_subresource_name_is_still_a_list():
    # scylla-manager lists prefixes like .../table/geocoded_locations/
    for word in ("location", "versioning", "uploads", "delete", "tagging"):
        q = f"list-type=2&prefix=keyspace%2Fmain%2Ftable%2Fgeocoded_{word}s%2F"
        assert _route("GET", "/bkt", q) == "v2", word
        assert _route("GET", "/bkt", f"prefix=x_{word}%2F&delimiter=%2F") == "v1", word


if __name__ == "__main__":
    test_v1_list_with_params_routes_to_v1_handler()
    test_bare_bucket_get_is_v1_list()
    test_v2_list_still_routes_to_v2()
    test_subresource_get_still_forwards()
    print("ok")
    test_prefix_containing_subresource_name_is_still_a_list()
