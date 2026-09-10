"""Regression coverage for generation publication and immutable part attempts."""

import hashlib
import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import ClientError
from fastapi import Request

from s3proxy import crypto
from s3proxy.errors import S3Error
from s3proxy.handlers import S3ProxyHandler
from s3proxy.state import MultipartStateManager
from s3proxy.state.metadata import load_multipart_metadata
from s3proxy.streaming.chunked import decode_aws_chunked_stream


def request(method="PUT", body=b"", headers=None, query=""):
    headers = dict(headers or {})
    headers.setdefault("content-length", str(len(body)))

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": method,
            "path": "/bucket/key",
            "raw_path": b"/bucket/key",
            "query_string": query.encode(),
            "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
        },
        receive,
    )


@pytest.fixture
def proxy(settings, mock_s3):
    handler = S3ProxyHandler(settings, {}, MultipartStateManager())
    handler._client = lambda _: mock_s3
    return handler


async def consume(response):
    if hasattr(response, "body_iterator"):
        try:
            return b"".join([chunk async for chunk in response.body_iterator])
        finally:
            if getattr(response, "cleanup", None):
                await response.cleanup()
    return response.body


async def create(proxy, credentials):
    response = await proxy.handle_create_multipart_upload(request("POST"), credentials)
    return next(e.text for e in ET.fromstring(response.body).iter() if e.tag.endswith("UploadId"))


def completion(upload, parts):
    body = (
        "<CompleteMultipartUpload>"
        + "".join(
            f"<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>" for n, etag in parts
        )
        + "</CompleteMultipartUpload>"
    )
    return request("POST", body.encode(), query=f"uploadId={upload}")


async def put_part(proxy, credentials, upload, number, data, sha=None):
    return await proxy.handle_upload_part(
        request(
            body=data,
            headers={"x-amz-content-sha256": sha or "UNSIGNED-PAYLOAD"},
            query=f"uploadId={upload}&partNumber={number}",
        ),
        credentials,
    )


async def test_wrong_hash_cannot_replace_buffered_object(proxy, credentials, mock_s3):
    await proxy.handle_put_object(request(body=b"before"), credentials)
    before = mock_s3.objects["bucket/key"]["Body"]
    with pytest.raises(S3Error):
        await proxy.handle_put_object(
            request(
                body=b"after",
                headers={"x-amz-content-sha256": hashlib.sha256(b"different").hexdigest()},
            ),
            credentials,
        )
    assert mock_s3.objects["bucket/key"]["Body"] == before


async def test_multipart_to_buffered_overwrite(proxy, credentials):
    await proxy.handle_put_object(
        request(body=b"old-content", headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}),
        credentials,
    )
    await proxy.handle_put_object(request(body=b"new"), credentials)
    head = await proxy.handle_head_object(request("HEAD"), credentials)
    assert head.headers["content-length"] == "3"
    assert await consume(await proxy.handle_get_object(request("GET"), credentials)) == b"new"


async def test_unknown_complete_does_not_succeed_for_existing_object(proxy, credentials):
    await proxy.handle_put_object(
        request(body=b"old", headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}), credentials
    )
    with pytest.raises(S3Error):
        await proxy.handle_complete_multipart_upload(
            completion("never-created", [(1, "bad")]), credentials
        )


async def test_rejected_part_preserves_accepted_attempt(proxy, credentials):
    upload = await create(proxy, credentials)
    good = await put_part(proxy, credentials, upload, 1, b"before")
    old = (await proxy.multipart_manager.get_upload("bucket", "key", upload)).parts[1]
    with pytest.raises(S3Error):
        await put_part(
            proxy, credentials, upload, 1, b"after", hashlib.sha256(b"wrong").hexdigest()
        )
    current = (await proxy.multipart_manager.get_upload("bucket", "key", upload)).parts[1]
    assert current.staging_key == old.staging_key
    await proxy.handle_complete_multipart_upload(
        completion(upload, [(1, good.headers["etag"])]), credentials
    )
    assert await consume(await proxy.handle_get_object(request("GET"), credentials)) == b"before"


async def test_out_of_order_parts_with_short_logical_tail(proxy, credentials):
    upload = await create(proxy, credentials)
    tail = await put_part(proxy, credentials, upload, 2, b"tail")
    data = b"A" * (9 * 1024**2)
    first = await put_part(proxy, credentials, upload, 1, data)
    complete_request = completion(upload, [(1, first.headers["etag"]), (2, tail.headers["etag"])])
    response = await proxy.handle_complete_multipart_upload(complete_request, credentials)
    assert response.status_code == 200
    get = await proxy.handle_get_object(request("GET"), credentials)
    head = await proxy.handle_head_object(request("HEAD"), credentials)
    assert get.headers["etag"] == head.headers["etag"]
    assert await consume(get) == data + b"tail"
    assert (
        await proxy.handle_complete_multipart_upload(
            completion(upload, [(1, first.headers["etag"]), (2, tail.headers["etag"])]), credentials
        )
    ).status_code == 200


async def test_metadata_failure_never_becomes_plaintext(proxy, credentials, mock_s3, monkeypatch):
    await proxy.handle_put_object(
        request(body=b"old", headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}), credentials
    )
    monkeypatch.setattr(
        mock_s3,
        "get_object",
        AsyncMock(side_effect=ClientError({"Error": {"Code": "ServiceUnavailable"}}, "GetObject")),
    )
    with pytest.raises(ClientError):
        await load_multipart_metadata(mock_s3, "bucket", "key")


async def test_sidecar_failure_does_not_publish_object(proxy, credentials, mock_s3, monkeypatch):
    original = mock_s3.put_object

    async def fail_metadata(bucket, key, *args, **kwargs):
        if "generations/" in key:
            raise RuntimeError("injected metadata failure")
        return await original(bucket, key, *args, **kwargs)

    monkeypatch.setattr(mock_s3, "put_object", fail_metadata)
    with pytest.raises(S3Error):
        await proxy.handle_put_object(
            request(body=b"new", headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}), credentials
        )
    assert "bucket/key" not in mock_s3.objects


@pytest.mark.parametrize("body", [b"3\r\nabc\r\n5\r\nxy", b"3\r\nabcXX0\r\n\r\n", b"0\r\n"])
async def test_truncated_or_malformed_chunks_rejected(body):
    with pytest.raises(S3Error):
        _ = [chunk async for chunk in decode_aws_chunked_stream(request(body=body))]


def test_new_frame_attempts_do_not_reuse_nonce():
    dek = crypto.generate_dek()
    first = crypto.encrypt_frame(b"AAAA", dek, "upload", 1, 0)
    second = crypto.encrypt_frame(b"BBBB", dek, "upload", 1, 0)
    assert first[:12] != second[:12]
    assert crypto.decrypt(first, dek) == b"AAAA"


async def test_cancelled_publication_keeps_possibly_accepted_stage(
    proxy, credentials, mock_s3, monkeypatch
):
    import asyncio

    upload = await create(proxy, credentials)
    original = proxy.multipart_manager.add_part

    async def committed_then_cancelled(*args):
        await original(*args)
        raise asyncio.CancelledError

    monkeypatch.setattr(proxy.multipart_manager, "add_part", committed_then_cancelled)
    with pytest.raises(asyncio.CancelledError):
        await put_part(proxy, credentials, upload, 1, b"accepted")
    state = await proxy.multipart_manager.get_upload("bucket", "key", upload)
    assert f"bucket/{state.parts[1].staging_key}" in mock_s3.objects
    await proxy.handle_complete_multipart_upload(
        completion(upload, [(1, state.parts[1].md5)]), credentials
    )
    assert await consume(await proxy.handle_get_object(request("GET"), credentials)) == b"accepted"


async def test_completed_copy_does_not_prove_destination_upload_identity(proxy, credentials):
    upload = await create(proxy, credentials)
    part = await put_part(proxy, credentials, upload, 1, b"source")
    await proxy.handle_complete_multipart_upload(
        completion(upload, [(1, part.headers["etag"])]), credentials
    )
    copy = request(headers={"x-amz-copy-source": "/bucket/key"})
    copy.scope["path"] = "/bucket/destination"
    await proxy.handle_copy_object(copy, credentials)
    complete = completion(upload, [(1, part.headers["etag"])])
    complete.scope["path"] = "/bucket/destination"
    with pytest.raises(S3Error):
        await proxy.handle_complete_multipart_upload(complete, credentials)


async def test_abort_removes_accepted_and_replaced_attempts(proxy, credentials, mock_s3):
    upload = await create(proxy, credentials)
    await put_part(proxy, credentials, upload, 1, b"first")
    await put_part(proxy, credentials, upload, 1, b"replacement")
    assert sum("/attempts/" in k for k in mock_s3.objects) == 2
    await proxy.handle_abort_multipart_upload(
        request("DELETE", query=f"uploadId={upload}"), credentials
    )
    assert not any("/attempts/" in k for k in mock_s3.objects)


async def test_legacy_active_copy_is_rejected(proxy, credentials):
    await proxy.multipart_manager.create_upload("bucket", "key", "legacy", crypto.generate_dek())
    with pytest.raises(S3Error, match="Legacy in-flight"):
        await proxy.handle_upload_part_copy(
            request(query="uploadId=legacy&partNumber=1"), credentials
        )


async def test_failed_complete_keeps_upload_retryable(proxy, credentials, monkeypatch):
    upload = await create(proxy, credentials)
    part = await put_part(proxy, credentials, upload, 1, b"retry-me")
    import s3proxy.handlers.multipart.staged as staged

    save = staged.save_multipart_metadata
    with monkeypatch.context() as patch:
        patch.setattr(
            staged, "save_multipart_metadata", AsyncMock(side_effect=RuntimeError("failed"))
        )
        with pytest.raises(RuntimeError):
            await proxy.handle_complete_multipart_upload(
                completion(upload, [(1, part.headers["etag"])]), credentials
            )
    assert staged.save_multipart_metadata is save
    assert await proxy.multipart_manager.get_upload("bucket", "key", upload) is not None
    await proxy.handle_complete_multipart_upload(
        completion(upload, [(1, part.headers["etag"])]), credentials
    )
    assert await consume(await proxy.handle_get_object(request("GET"), credentials)) == b"retry-me"


@pytest.mark.parametrize("copy_first", [True, False])
async def test_key_selection_cannot_change_after_first_writer(
    proxy, credentials, mock_s3, copy_first
):
    source = request(body=b"source")
    source.scope["path"] = "/bucket/source"
    await proxy.handle_put_object(source, credentials)
    upload = await create(proxy, credentials)
    state = await proxy.multipart_manager.get_upload("bucket", "key", upload)
    original_dek = state.dek
    if not copy_first:
        await put_part(proxy, credentials, upload, 1, b"initial")
    copy = request(
        headers={"x-amz-copy-source": "/bucket/source"}, query=f"uploadId={upload}&partNumber=1"
    )
    response = await proxy.handle_upload_part_copy(copy, credentials)
    await consume(response)
    state = await proxy.multipart_manager.get_upload("bucket", "key", upload)
    chosen = state.dek
    if not copy_first:
        assert chosen == original_dek
    else:
        assert chosen != original_dek
        assert (
            mock_s3.objects[f"bucket/{state.parts[1].staging_key}"]["Body"]
            == mock_s3.objects["bucket/source"]["Body"]
        )
    replacement = await put_part(proxy, credentials, upload, 1, b"replacement")
    assert (await proxy.multipart_manager.get_upload("bucket", "key", upload)).dek == chosen
    await proxy.handle_complete_multipart_upload(
        completion(upload, [(1, replacement.headers["etag"])]), credentials
    )
    assert (
        await consume(await proxy.handle_get_object(request("GET"), credentials)) == b"replacement"
    )


async def test_empty_streaming_put_roundtrip(proxy, credentials):
    await proxy.handle_put_object(
        request(body=b"", headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}), credentials
    )
    assert await consume(await proxy.handle_get_object(request("GET"), credentials)) == b""
