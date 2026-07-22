"""CompleteMultipartUpload retry + idempotency.

Prod incident 2026-07-22: Hetzner streams a 200 for CompleteMultipartUpload
then fails mid-response with an embedded ``InternalError`` ("The server did
not respond in time.") - a documented S3-family quirk for this operation.
Nothing retried it: botocore's own retry checker only treats HTTP 5xx status
codes and a small error-code allowlist as transient, and neither covers a 200
response with an error body, and the handler bare-``raise``d everything except
``EntityTooSmall``. rclone saw the failure and had to re-upload whole
multi-GB SSTables.

A plain retry isn't safe on its own: if the backend actually finished
assembling the object before the response died, the ``upload_id`` is already
invalidated, so retrying surfaces ``NoSuchUpload`` even though the object is
fine. These tests pin both halves: the retry-on-transient-error behavior and
the head_object-based check that stops a retry from turning a successful
upload into a false failure.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3proxy.handlers.multipart import lifecycle
from s3proxy.state import PartMetadata

COMPLETED_PARTS = [
    PartMetadata(
        part_number=1, plaintext_size=100, ciphertext_size=128, etag="etag-1", md5="etag-1"
    ),
    PartMetadata(
        part_number=2, plaintext_size=100, ciphertext_size=128, etag="etag-2", md5="etag-2"
    ),
]
S3_PARTS = [{"PartNumber": 1, "ETag": '"etag-1"'}, {"PartNumber": 2, "ETag": '"etag-2"'}]
EXPECTED_CIPHERTEXT_SIZE = sum(p.ciphertext_size for p in COMPLETED_PARTS)


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(lifecycle, "COMPLETE_RETRY_BACKOFF_SEC", 0.0)


def _client_error(code: str, message: str | None = None) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message or code}}, "CompleteMultipartUpload"
    )


class _FlakyCompleteClient:
    """Fake backend client for exercising complete_multipart_upload retry paths.

    fail_times: number of leading calls that raise a transient error without
        actually assembling the object.
    error_code: the transient error code those calls raise.
    phantom_success_on_first: the first call raises InternalError *after* the
        backend actually finished assembling the object server-side (the "200
        then dies mid-response" case) - the retry then hits NoSuchUpload
        because the upload_id is already gone.
    abort_after_first_failure: the object is never assembled and the upload
        is separately aborted/expired before the retry - retry hits
        NoSuchUpload for real.
    """

    def __init__(
        self,
        *,
        fail_times: int = 0,
        error_code: str = "InternalError",
        phantom_success_on_first: bool = False,
        abort_after_first_failure: bool = False,
    ) -> None:
        self.calls = 0
        self.head_calls = 0
        self.fail_times = fail_times
        self.error_code = error_code
        self.phantom_success_on_first = phantom_success_on_first
        self.abort_after_first_failure = abort_after_first_failure
        self.object_assembled = False

    async def complete_multipart_upload(self, bucket, key, upload_id, parts):
        self.calls += 1

        if self.phantom_success_on_first:
            if self.calls == 1:
                self.object_assembled = True
                raise _client_error("InternalError", "The server did not respond in time.")
            raise _client_error("NoSuchUpload", "The specified upload does not exist")

        if self.abort_after_first_failure:
            if self.calls == 1:
                raise _client_error("InternalError", "The server did not respond in time.")
            raise _client_error("NoSuchUpload", "The specified upload does not exist")

        if self.calls <= self.fail_times:
            raise _client_error(self.error_code)

        self.object_assembled = True
        return {"ETag": '"final-etag"'}

    async def head_object(self, bucket, key):
        self.head_calls += 1
        if self.object_assembled:
            return {"ContentLength": EXPECTED_CIPHERTEXT_SIZE, "ETag": '"final-etag"'}
        raise _client_error("NoSuchKey", "Not Found")


@pytest.mark.asyncio
async def test_retries_transient_error_then_succeeds(handler):
    client = _FlakyCompleteClient(fail_times=2)

    resp = await handler._complete_multipart_upload_with_retry(
        client, "bucket", "key", "upload-1", S3_PARTS, COMPLETED_PARTS
    )

    assert resp == {"ETag": '"final-etag"'}
    assert client.calls == 3


@pytest.mark.asyncio
async def test_gives_up_after_max_attempts(handler):
    client = _FlakyCompleteClient(fail_times=lifecycle.COMPLETE_RETRY_ATTEMPTS + 10)

    with pytest.raises(ClientError) as exc:
        await handler._complete_multipart_upload_with_retry(
            client, "bucket", "key", "upload-1", S3_PARTS, COMPLETED_PARTS
        )

    assert exc.value.response["Error"]["Code"] == "InternalError"
    assert client.calls == lifecycle.COMPLETE_RETRY_ATTEMPTS
    assert client.object_assembled is False


@pytest.mark.asyncio
async def test_does_not_retry_non_retryable_error(handler):
    client = _FlakyCompleteClient(fail_times=1, error_code="AccessDenied")

    with pytest.raises(ClientError) as exc:
        await handler._complete_multipart_upload_with_retry(
            client, "bucket", "key", "upload-1", S3_PARTS, COMPLETED_PARTS
        )

    assert exc.value.response["Error"]["Code"] == "AccessDenied"
    assert client.calls == 1


@pytest.mark.asyncio
async def test_recovers_via_head_object_when_prior_attempt_already_finished(handler):
    """The exact prod failure: backend finished the assembly, the client only saw
    the error, and a naive retry would otherwise report a false failure."""
    client = _FlakyCompleteClient(phantom_success_on_first=True)

    resp = await handler._complete_multipart_upload_with_retry(
        client, "bucket", "key", "upload-1", S3_PARTS, COMPLETED_PARTS
    )

    assert resp == {"ETag": '"final-etag"'}
    assert client.calls == 2
    assert client.head_calls == 1


@pytest.mark.asyncio
async def test_does_not_mask_a_real_failure_as_success(handler):
    """NoSuchUpload after a retry must still fail if the object was never
    actually assembled - the head_object check must not paper over genuine
    failures."""
    client = _FlakyCompleteClient(abort_after_first_failure=True)

    with pytest.raises(ClientError) as exc:
        await handler._complete_multipart_upload_with_retry(
            client, "bucket", "key", "upload-1", S3_PARTS, COMPLETED_PARTS
        )

    assert exc.value.response["Error"]["Code"] == "NoSuchUpload"
    assert client.head_calls == 1
