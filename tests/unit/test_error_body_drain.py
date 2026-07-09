"""Error responses must not leave unread request bodies on keep-alive connections.

A request rejected before its body is read (e.g. UploadPart with an
out-of-range part number) leaves the body bytes buffered on the connection;
the client's next request on that connection then gets a raw uvicorn
400 Bad Request (this is how test_part_number_out_of_range broke in CI: the
abort following the rejected upload failed). Small bodies are drained so
keep-alive stays usable; large ones get Connection: close instead of being
slurped.
"""

from fastapi.testclient import TestClient

from s3proxy import concurrency
from s3proxy.app import create_app


def _client(settings):
    return TestClient(create_app(settings), raise_server_exceptions=False)


def test_error_with_small_body_keeps_connection_alive(settings):
    with _client(settings) as client:
        resp = client.put("/bucket/key", content=b"x" * 16)
        assert resp.status_code == 403
        assert "connection" not in resp.headers


def test_error_with_large_body_closes_connection(settings):
    with _client(settings) as client:
        resp = client.put("/bucket/key", content=b"x" * (concurrency.MAX_BUFFER_SIZE + 1))
        assert resp.status_code == 403
        assert resp.headers.get("connection") == "close"
