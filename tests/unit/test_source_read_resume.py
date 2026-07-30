"""read_source_bytes resumes a truncated body instead of restarting it.

Prod incident 2026-07-29: the Scylla backup of `main.companies` failed on 10 of
13 racks. Hetzner terminates a response mid-body with a clean TCP FIN while
still advertising the full Content-Length, so aiohttp raises ClientPayloadError
after delivering only part of the payload. All 327 captured occurrences were
clean closes (no ConnectionError suffix), i.e. the sender deliberately ended the
response rather than a network fault.

Measured at ~0.5% per read (2 of 400 in a sustained 16-way probe), and the
truncation always lands on a page boundary -- only two values ever appeared:

    190 x  received 4165632 of 8388636   (1017 x 4096)
    137 x  received 8331264 of 8388636   (2034 x 4096)

Re-requesting the identical range reproduces the identical truncation (9 of 12
retried ranges truncated at the same offset every attempt), so plain retry
cannot make progress -- which is why ~12% of reads never recovered no matter how
many attempts they were given. At ~572 reads per 4.7GB copy a single large file
had a ~94% chance of hitting at least one, so `companies` failed nearly every run
while smaller tables passed.

Resuming works because a fresh request gets a fresh response buffer. Verified
against production: resuming 4 real failing ranges returned exactly the missing
bytes, and the stitched result was SHA-256 identical to an untruncated read.
"""

from __future__ import annotations

import aiohttp
import pytest
from botocore.exceptions import ClientError

from s3proxy.handlers import base
from s3proxy.handlers.base import read_source_bytes

BODY = bytes(range(256)) * 512  # 131072 bytes, non-uniform so stitching is checked


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(base, "SOURCE_READ_BACKOFF_SEC", 0.0)


class _Body:
    """Yields `cut` bytes then raises, mimicking a mid-body FIN."""

    def __init__(self, data: bytes, cut: int | None):
        self._data = data
        self._cut = cut
        self._sent = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def read(self, n: int = -1) -> bytes:
        limit = len(self._data) if self._cut is None else self._cut
        if self._sent >= limit:
            if self._cut is not None:
                raise aiohttp.ClientPayloadError(
                    "Response payload is not completed: ContentLengthError(400, "
                    'message="Not enough data to satisfy content length header '
                    f'(received {self._cut} of {len(self._data)} bytes)."))'
                )
            return b""
        end = limit if n < 0 else min(self._sent + n, limit)
        chunk = self._data[self._sent : end]
        self._sent = end
        return chunk


class _Client:
    """Serves BODY, truncating each successive request per `cuts`."""

    def __init__(self, cuts: list[int | None]):
        self._cuts = list(cuts)
        self.ranges: list[str | None] = []

    async def get_object(self, bucket, key, range_header=None):
        self.ranges.append(range_header)
        start, end = 0, len(BODY) - 1
        if range_header:
            start, end = base._parse_byte_range(range_header)
            if end is None:
                end = len(BODY) - 1
        segment = BODY[start : end + 1]
        cut = self._cuts.pop(0) if self._cuts else None
        return {"Body": _Body(segment, cut)}


@pytest.mark.asyncio
async def test_clean_read_needs_one_request():
    c = _Client([None])
    assert await read_source_bytes(c, "b", "k") == BODY
    assert c.ranges == [None]


@pytest.mark.asyncio
async def test_resumes_ranged_read_and_stitches_identically():
    """The prod shape: full range truncated once, then resumed."""
    c = _Client([50_000, None])
    got = await read_source_bytes(c, "b", "k", "bytes=0-131071")
    assert got == BODY, "stitched result must equal an untruncated read"
    assert c.ranges == ["bytes=0-131071", "bytes=50000-131071"]


@pytest.mark.asyncio
async def test_resume_offsets_are_absolute_not_relative():
    """A non-zero range start must not be dropped when resuming."""
    c = _Client([1000, None])
    got = await read_source_bytes(c, "b", "k", "bytes=4096-20479")
    assert got == BODY[4096:20480]
    assert c.ranges == ["bytes=4096-20479", "bytes=5096-20479"]


@pytest.mark.asyncio
async def test_repeated_truncation_still_converges():
    """Several truncations in a row: each attempt keeps prior progress."""
    c = _Client([40_000, 30_000, 20_000, None])
    got = await read_source_bytes(c, "b", "k", "bytes=0-131071")
    assert got == BODY
    assert c.ranges == [
        "bytes=0-131071",
        "bytes=40000-131071",
        "bytes=70000-131071",
        "bytes=90000-131071",
    ]


@pytest.mark.asyncio
async def test_whole_object_read_resumes_by_offset():
    """No range requested: resume must still target the missing tail."""
    c = _Client([70_000, None])
    got = await read_source_bytes(c, "b", "k")
    assert got == BODY
    assert c.ranges == [None, "bytes=70000-"]


@pytest.mark.asyncio
async def test_open_ended_range_preserved_on_resume():
    c = _Client([2048, None])
    got = await read_source_bytes(c, "b", "k", "bytes=1024-")
    assert got == BODY[1024:]
    assert c.ranges == ["bytes=1024-", "bytes=3072-"]


@pytest.mark.asyncio
async def test_gives_up_after_attempt_cap():
    c = _Client([10_000] * base.SOURCE_READ_ATTEMPTS)
    with pytest.raises(aiohttp.ClientPayloadError):
        await read_source_bytes(c, "b", "k", "bytes=0-131071")
    assert len(c.ranges) == base.SOURCE_READ_ATTEMPTS


@pytest.mark.asyncio
async def test_non_retryable_error_is_not_resumed():
    class _Boom:
        async def get_object(self, *a, **k):
            raise ValueError("permanent")

    with pytest.raises(ValueError):
        await read_source_bytes(_Boom(), "b", "k", "bytes=0-10")


def test_parse_byte_range():
    assert base._parse_byte_range("bytes=0-99") == (0, 99)
    assert base._parse_byte_range("bytes=4096-") == (4096, None)
    assert base._parse_byte_range("4096-8191") == (4096, 8191)


# --- 504 GatewayTimeout retryability ---------------------------------------
#
# 273 of 288 fatal copy failures in the 2026-07-29 capture were HTTP 504
# GatewayTimeout on UploadPartCopy. The segment loop in copy.py already retries
# via is_retryable_source_error, but 504 was absent from the retryable set, so it
# hit `raise` on the first occurrence -- which is why only 4 SEGMENT_RETRY events
# were logged against 273 fatal 504s.
#
# Reproduced: 572 UploadPartCopy calls at 8-way concurrency against the real
# object yielded 1 x 504 (0.17%), with latency p50 1.14s / p90 3.31s / max
# 61.86s. Heavy-tailed latency means a 504 is transient congestion. At 0.17% per
# copy a 4.7GB file (572 copies) has a ~63% chance of hitting one.


@pytest.mark.parametrize(
    "code",
    ["GatewayTimeout", "504", "BadGateway", "502", "503", "500", "InternalError", "SlowDown"],
)
def test_transient_backend_codes_are_retryable(code):
    exc = ClientError(
        {"Error": {"Code": code, "Message": "x"}, "ResponseMetadata": {"HTTPStatusCode": 504}},
        "UploadPartCopy",
    )
    assert base.is_retryable_source_error(exc) is True


@pytest.mark.parametrize("code", ["AccessDenied", "NoSuchKey", "InvalidPart", "EntityTooSmall"])
def test_permanent_codes_are_not_retryable(code):
    exc = ClientError({"Error": {"Code": code, "Message": "x"}}, "UploadPartCopy")
    assert base.is_retryable_source_error(exc) is False


@pytest.mark.asyncio
async def test_source_read_retries_gateway_timeout():
    """A 504 mid-read must be retried, not raised on first sight."""

    class _Flaky:
        def __init__(self):
            self.calls = 0

        async def get_object(self, bucket, key, range_header=None):
            self.calls += 1
            if self.calls == 1:
                raise ClientError(
                    {
                        "Error": {"Code": "GatewayTimeout", "Message": "did not respond in time"},
                        "ResponseMetadata": {"HTTPStatusCode": 504},
                    },
                    "GetObject",
                )
            return {"Body": _Body(BODY, None)}

    c = _Flaky()
    assert await read_source_bytes(c, "b", "k", "bytes=0-131071") == BODY
    assert c.calls == 2


# --- message-less 400 retryability -----------------------------------------
#
# Prod 2026-07-30: 4/4 UploadPart failures on main.companies arrived as
# InvalidArgument with Message=None, on legal 50MiB non-final parts (part 2-16,
# never part 1). A real InvalidArgument always names the offending argument, so
# the message-less form is Hetzner shedding load, not a malformed request.
#
# Reproduced: 24 x 50MiB parts at 16-way concurrency yielded 1 failure, surfaced
# as GatewayTimeout("The server did not respond in time"), with latency p50
# 13.09s / max 52.32s -- against p50 1.79s for the same parts unloaded. Same
# congestion event, two different bodies depending on whether RGW managed to
# write one.


def _upload_part_error(code, message=None):
    err = {"Code": code}
    if message is not None:
        err["Message"] = message
    return ClientError({"Error": err}, "UploadPart")


@pytest.mark.parametrize("code", ["InvalidArgument", "BadRequest", "400"])
def test_message_less_400_is_retryable(code):
    assert base.is_retryable_source_error(_upload_part_error(code)) is True
    assert base.is_retryable_source_error(_upload_part_error(code, "")) is True


def test_invalid_argument_with_reason_still_fails():
    """A genuine validation error must not be retried forever."""
    exc = _upload_part_error(
        "InvalidArgument", "Part number must be an integer between 1 and 10000"
    )
    assert base.is_retryable_source_error(exc) is False


@pytest.mark.parametrize(
    "code,msg",
    [
        ("AccessDenied", "Access Denied"),
        ("NoSuchKey", "The specified key does not exist."),
        ("EntityTooSmall", "Your proposed upload is smaller than the minimum allowed size"),
        ("InvalidPart", "One or more of the specified parts could not be found"),
    ],
)
def test_permanent_errors_with_messages_not_retried(code, msg):
    assert base.is_retryable_source_error(_upload_part_error(code, msg)) is False
