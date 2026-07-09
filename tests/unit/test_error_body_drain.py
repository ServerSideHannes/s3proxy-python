"""Error responses must not leave unread request bodies on keep-alive connections.

A small request rejected before its body is read (e.g. UploadPart with an
out-of-range part number) leaves the body bytes unconsumed; the client's next
request on that connection then gets a raw uvicorn 400 (this is how
test_part_number_out_of_range broke in CI: the abort following the rejected
upload failed). The error handler drains small bodies. Large bodies are left
for uvicorn to discard -- actively closing the connection instead resets
clients that send the whole body before reading the response (covered by
tests/integration/test_presigned_put_e2e.py::test_bad_signature_rejected).
The end-to-end connection-reuse semantics are integration-tested; these tests
pin the handler behavior itself.
"""

import xml.etree.ElementTree as ET

from fastapi.testclient import TestClient

from s3proxy import concurrency
from s3proxy.app import create_app


def _client(settings):
    return TestClient(create_app(settings), raise_server_exceptions=False)


def test_error_with_small_body_is_drained_and_keeps_connection_alive(settings):
    with _client(settings) as client:
        resp = client.put("/bucket/key", content=b"x" * 16)
        assert resp.status_code == 403
        assert "connection" not in resp.headers
        assert ET.fromstring(resp.content).tag == "Error"


def test_error_with_large_body_is_not_slurped_and_not_closed(settings):
    with _client(settings) as client:
        resp = client.put("/bucket/key", content=b"x" * (concurrency.MAX_BUFFER_SIZE + 1))
        assert resp.status_code == 403
        assert "connection" not in resp.headers
        assert ET.fromstring(resp.content).tag == "Error"
