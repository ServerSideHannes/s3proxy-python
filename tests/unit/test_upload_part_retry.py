"""UploadPart retry on transient backend errors.

Prod incident 2026-07-28: the Scylla backup of `main.companies` failed on 10 of
13 racks, leaving ~2.4TiB un-uploaded. Every failure was a ~6.2GiB SSTable and
every error was ``InternalError: The server did not respond in time.`` with
``status code: 200`` -- RGW commits to a 200, streams it, then puts the failure
in the body.

Three retry layers all miss that shape because each decides from the HTTP status
line, which already read 200:
  * rclone's ``LowLevelRetries`` gates on status 429/500/503 (backend/s3/s3.go)
  * rclone's string fallback only matches transport phrases ("use of closed
    network connection", "unexpected EOF reading trailer"), not ``InternalError``
  * botocore's ``max_attempts`` in S3Client sees the same 200

#133 added this retry to UploadPartCopy and #138 to CompleteMultipartUpload, but
UploadPart -- the operation a Scylla backup actually uses (all PUT, no COPY) --
was left bare. At 50MB rclone chunks a 6.2GiB file is ~762 internal PUTs, so one
unretried transient failure loses the whole multi-GB file.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3proxy.handlers import base
from s3proxy.handlers.multipart import upload_part as upload_part_mod
from s3proxy.handlers.multipart.upload_part import UploadPartMixin


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(base, "SOURCE_READ_BACKOFF_SEC", 0.0)


def _client_error(code: str, message: str | None = None) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message or code}}, "UploadPart")


class _FlakyUploadClient:
    """Backend whose first ``fail_times`` upload_part calls raise ``error_code``."""

    def __init__(self, fail_times: int = 0, error_code: str = "InternalError"):
        self._fail_times = fail_times
        self._error_code = error_code
        self.attempts = 0
        self.seen = []

    async def upload_part(self, bucket, key, upload_id, part_number, body):
        self.attempts += 1
        self.seen.append((part_number, bytes(body)))
        if self._fail_times > 0:
            self._fail_times -= 1
            if self._error_code == "InternalError":
                raise _client_error("InternalError", "The server did not respond in time.")
            raise _client_error(self._error_code)
        return {"ETag": f'"etag-{part_number}"'}


async def _upload(client, **kw):
    return await UploadPartMixin._upload_part_with_retry(
        UploadPartMixin.__new__(UploadPartMixin),
        client,
        "bucket",
        "key",
        "upload-id",
        kw.pop("internal_part_num", 7),
        kw.pop("ciphertext", b"ciphertext-bytes"),
        client_part_num=kw.pop("client_part_num", 1),
    )


@pytest.mark.asyncio
async def test_retries_late_200_internal_error():
    """The exact prod shape: InternalError in a 200 body, then success."""
    client = _FlakyUploadClient(fail_times=1)
    resp = await _upload(client)
    assert resp["ETag"] == '"etag-7"'
    assert client.attempts == 2


@pytest.mark.asyncio
async def test_retry_reuses_same_part_number_and_bytes():
    """Re-PUT must be idempotent - same part number, same ciphertext."""
    client = _FlakyUploadClient(fail_times=2)
    await _upload(client, internal_part_num=3, ciphertext=b"abc")
    assert client.attempts == 3
    assert client.seen == [(3, b"abc")] * 3


@pytest.mark.asyncio
async def test_gives_up_after_attempt_cap():
    """Exhausting attempts re-raises rather than looping forever."""
    client = _FlakyUploadClient(fail_times=base.SOURCE_READ_ATTEMPTS)
    with pytest.raises(ClientError):
        await _upload(client)
    assert client.attempts == base.SOURCE_READ_ATTEMPTS


@pytest.mark.asyncio
async def test_non_retryable_error_is_not_retried():
    """A real client error must fail fast, not burn attempts."""
    client = _FlakyUploadClient(fail_times=1, error_code="AccessDenied")
    with pytest.raises(ClientError):
        await _upload(client)
    assert client.attempts == 1


@pytest.mark.asyncio
async def test_success_path_makes_one_call():
    client = _FlakyUploadClient()
    resp = await _upload(client)
    assert resp["ETag"] == '"etag-7"'
    assert client.attempts == 1


@pytest.mark.parametrize("code", ["InternalError", "SlowDown", "ServiceUnavailable"])
@pytest.mark.asyncio
async def test_retryable_codes(code):
    client = _FlakyUploadClient(fail_times=1, error_code=code)
    await _upload(client)
    assert client.attempts == 2


# --- message-less 400 + observability ---------------------------------------
#
# Prod 2026-07-30: the same `main.companies` upload failed again, this time as
# `InvalidArgument` with **Message=None** (4/4 occurrences) on legal 50MiB
# non-final parts (parts 2-16, never part 1). `GatewayTimeout` was already
# retryable from #133, but the message-less variant was not, so it fell through
# to raise on first sight.
#
# Both are the same congestion event: a 24 x 50MiB probe at 16-way concurrency
# reproduced 1 failure surfaced as GatewayTimeout("The server did not respond in
# time"), with latency p50 13.09s / max 52.32s against p50 1.79s unloaded --
# Hetzner sheds load and only sometimes manages to write an error body.


class _MessagelessClient:
    """Raises InvalidArgument with no Message, the prod 2026-07-30 shape."""

    def __init__(self, fail_times: int = 1):
        self._fail_times = fail_times
        self.attempts = 0

    async def upload_part(self, bucket, key, upload_id, part_number, body):
        self.attempts += 1
        if self._fail_times > 0:
            self._fail_times -= 1
            raise ClientError({"Error": {"Code": "InvalidArgument"}}, "UploadPart")
        return {"ETag": f'"etag-{part_number}"'}


@pytest.mark.asyncio
async def test_retries_messageless_invalid_argument():
    client = _MessagelessClient(fail_times=1)
    resp = await _upload(client)
    assert resp["ETag"] == '"etag-7"'
    assert client.attempts == 2


@pytest.mark.asyncio
async def test_real_invalid_argument_is_not_retried():
    """A validation error that names its argument must fail immediately."""

    class _Rejecting:
        def __init__(self):
            self.attempts = 0

        async def upload_part(self, *a, **k):
            self.attempts += 1
            raise _client_error(
                "InvalidArgument", "Part number must be an integer between 1 and 10000"
            )

    client = _Rejecting()
    with pytest.raises(ClientError):
        await _upload(client)
    assert client.attempts == 1


@pytest.mark.asyncio
async def test_recovered_upload_is_logged(monkeypatch):
    """A part that only lands after a retry must leave a trace."""
    events = []
    monkeypatch.setattr(
        upload_part_mod.logger,
        "warning",
        lambda ev, **kw: events.append((ev, kw)),
    )
    client = _FlakyUploadClient(fail_times=2)
    await _upload(client)
    names = [e for e, _ in events]
    assert "UPLOAD_PART_RECOVERED" in names
    recovered = next(kw for e, kw in events if e == "UPLOAD_PART_RECOVERED")
    assert recovered["attempts"] == 3


@pytest.mark.asyncio
async def test_exhaustion_distinguishable_from_rejection(monkeypatch):
    """Exhausted transient retries log exhausted=True; outright rejection False."""
    events = []
    monkeypatch.setattr(
        upload_part_mod.logger, "error", lambda ev, **kw: events.append((ev, kw))
    )
    monkeypatch.setattr(upload_part_mod.logger, "warning", lambda ev, **kw: None)

    client = _FlakyUploadClient(fail_times=99, error_code="InternalError")
    with pytest.raises(ClientError):
        await _upload(client)
    ev, kw = next((e, k) for e, k in events if e == "UPLOAD_PART_GAVE_UP")
    assert kw["exhausted"] is True
    assert kw["classified_retryable"] is True
    assert kw["attempts"] == base.SOURCE_READ_ATTEMPTS
    assert client.attempts == base.SOURCE_READ_ATTEMPTS

    events.clear()

    class _Denied:
        def __init__(self):
            self.attempts = 0

        async def upload_part(self, *a, **k):
            self.attempts += 1
            raise _client_error("AccessDenied", "Access Denied")

    denied = _Denied()
    with pytest.raises(ClientError):
        await _upload(denied)
    ev, kw = next((e, k) for e, k in events if e == "UPLOAD_PART_GAVE_UP")
    assert kw["exhausted"] is False
    assert kw["classified_retryable"] is False
    assert kw["aws_error_code"] == "AccessDenied"
    assert denied.attempts == 1
