"""CompleteMultipartUpload recovery path (issue #65 consolidation).

``handle_complete_multipart_upload`` no longer carries its own copy of the
"rebuild state by listing S3 parts" algorithm. When the manager has lost the
upload state it now delegates to the shared ``_recover_upload_state`` — the
exact path UploadPart already uses — which calls
``reconstruct_upload_state_from_s3`` and stores the result.

These tests pin the two behaviors that consolidation changed:
- state lost from the manager -> reconstructed from S3 -> completes and the
  assembled object is byte-identical to the encrypted parts.
- state unrecoverable (DEK gone from S3) -> raises ``NoSuchUpload`` instead of
  silently completing with partial/empty state (which would assemble a corrupt
  object).
"""

from urllib.parse import urlencode

import pytest

from s3proxy import crypto
from s3proxy.errors import S3Error
from s3proxy.state.metadata import load_multipart_metadata, persist_upload_state

# client part N owns internal parts [(N-1)*MAX + 1 .. N*MAX]; using one internal
# part per client part keeps the mapping obvious (internal 1 -> client 1, 21 -> 2).
_INTERNAL_FOR_CLIENT = {1: 1, 2: 21}


class _FakeURL:
    def __init__(self, path: str, query: str) -> None:
        self.path = path
        self.query = query


class _FakeRequest:
    """Minimal stand-in for fastapi.Request used by the complete handler."""

    def __init__(self, path: str, query: str, body: bytes) -> None:
        self.url = _FakeURL(path, query)
        self.headers: dict[str, str] = {}
        self._body = body

    async def body(self) -> bytes:
        return self._body


class _ClientCM:
    """Async context manager yielding a pre-built client (no real S3 session)."""

    def __init__(self, client) -> None:
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *exc) -> bool:
        return False


def _complete_request(
    bucket: str, key: str, upload_id: str, client_parts: list[int]
) -> _FakeRequest:
    parts_xml = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>&quot;etag-{n}&quot;</ETag></Part>"
        for n in client_parts
    )
    body = f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode()
    return _FakeRequest(f"/{bucket}/{key}", urlencode({"uploadId": upload_id}), body)


async def _seed_upload(mock_s3, dek: bytes, bucket: str, key: str, chunks: dict[int, bytes]) -> str:
    """Initiate a multipart upload in S3 and put encrypted internal parts.

    ``chunks`` maps client part number -> plaintext; each is encrypted exactly as
    the proxy does (deterministic per-part nonce) and stored under its internal
    part number.
    """
    resp = await mock_s3.create_multipart_upload(bucket, key)
    upload_id = resp["UploadId"]
    for client_part, plaintext in chunks.items():
        internal = _INTERNAL_FOR_CLIENT[client_part]
        nonce = crypto.derive_part_nonce(upload_id, internal)
        ciphertext = crypto.encrypt(plaintext, dek, nonce)
        await mock_s3.upload_part(bucket, key, upload_id, internal, ciphertext)
    return upload_id


class TestCompleteAfterStateLoss:
    async def test_reconstructs_from_s3_and_round_trips(
        self, handler, mock_s3, mock_s3_client, settings, credentials
    ):
        bucket, key = "test-bucket", "backups/db.bin"
        kid, kek = settings.keyring.key_for(credentials.access_key)
        dek = crypto.generate_dek()

        chunk1 = b"first-part-payload" * 64
        chunk2 = b"second-part-payload" * 32
        upload_id = await _seed_upload(mock_s3, dek, bucket, key, {1: chunk1, 2: chunk2})
        # DEK is the only thing reconstruction needs from S3 besides ListParts.
        await persist_upload_state(
            mock_s3_client, bucket, key, upload_id, crypto.wrap_key(dek, kek), kid
        )

        # Manager has no state (Redis flushed) -> handler must recover from S3.
        assert await handler.multipart_manager.get_upload(bucket, key, upload_id) is None
        handler._client = lambda creds: _ClientCM(mock_s3_client)

        resp = await handler.handle_complete_multipart_upload(
            _complete_request(bucket, key, upload_id, [1, 2]), credentials
        )

        assert resp.status_code == 200

        # The assembled object is the two encrypted parts concatenated in order,
        # and each segment decrypts back to the original plaintext.
        ct1 = crypto.encrypt(chunk1, dek, crypto.derive_part_nonce(upload_id, 1))
        ct2 = crypto.encrypt(chunk2, dek, crypto.derive_part_nonce(upload_id, 21))
        stored = mock_s3.objects[f"{bucket}/{key}"]["Body"]
        assert stored == ct1 + ct2
        assert crypto.decrypt(ct1, dek) == chunk1
        assert crypto.decrypt(ct2, dek) == chunk2

        meta = await load_multipart_metadata(mock_s3_client, bucket, key)
        assert meta is not None
        assert meta.part_count == 2
        assert meta.total_plaintext_size == len(chunk1) + len(chunk2)

    async def test_unrecoverable_state_raises_no_such_upload(
        self, handler, mock_s3, mock_s3_client, credentials
    ):
        bucket, key = "test-bucket", "orphan.bin"
        # Upload exists in S3 with parts, but the DEK state was never persisted,
        # so reconstruction cannot proceed.
        upload_id = await _seed_upload(
            mock_s3, crypto.generate_dek(), bucket, key, {1: b"data" * 16}
        )

        assert await handler.multipart_manager.get_upload(bucket, key, upload_id) is None
        handler._client = lambda creds: _ClientCM(mock_s3_client)

        with pytest.raises(S3Error) as exc:
            await handler.handle_complete_multipart_upload(
                _complete_request(bucket, key, upload_id, [1]), credentials
            )

        assert exc.value.code == "NoSuchUpload"
        # No partial / corrupt object must be written on the failed complete.
        assert f"{bucket}/{key}" not in mock_s3.objects
        assert upload_id in mock_s3.multipart_uploads
