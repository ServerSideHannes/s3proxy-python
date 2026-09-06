"""Dashboard encryption-status detection (issue #47 #6).

Multipart objects store the wrapped DEK in a sidecar object, not an on-object
``isec`` tag — the create-time metadata does not survive CompleteMultipartUpload.
The dashboard must consult the sidecar before reporting "not encrypted".
"""

from __future__ import annotations

import s3proxy.client as client_mod
from s3proxy.dashboard.collectors import _has_multipart_sidecar, list_bucket_objects
from s3proxy.state.metadata import save_multipart_metadata
from s3proxy.state.models import MultipartMetadata


async def test_sidecar_present_means_encrypted(mock_s3) -> None:
    bucket, key = "scylla-backups", "backup/sst/me-big-Data.db"
    await mock_s3.put_object(bucket, key, b"ciphertext")
    await save_multipart_metadata(
        mock_s3,
        bucket,
        key,
        MultipartMetadata(
            version=2,
            part_count=7,
            total_plaintext_size=1234,
            parts=[],
            wrapped_dek=b"wrapped-dek-bytes",
            kid="AKIA-TEST",
        ),
    )
    assert await _has_multipart_sidecar(mock_s3, bucket, key) is True


async def test_no_sidecar_means_not_detected(mock_s3) -> None:
    # A plain object with no sidecar — single-PUT objects carry their DEK as an
    # on-object tag instead, which head_object_detail checks separately.
    await mock_s3.put_object(bucket="b", key="plain.txt", body=b"data")
    assert await _has_multipart_sidecar(mock_s3, "b", "plain.txt") is False


async def test_sidecar_lookup_swallows_errors(mock_s3) -> None:
    # Missing object -> load raises internally -> detection returns False, no raise.
    assert await _has_multipart_sidecar(mock_s3, "b", "does-not-exist") is False


async def test_listing_annotates_per_object_encryption(mock_s3, settings) -> None:
    """list-style annotation: on-object tag, multipart sidecar, and plaintext."""
    from s3proxy.dashboard.collectors import _annotate_encryption

    # 1) single-PUT encrypted: on-object isec tag
    await mock_s3.put_object(
        bucket="b", key="single.bin", body=b"x", metadata={settings.dektag_name: "wrapped"}
    )
    # 2) multipart encrypted: sidecar, no on-object tag
    await mock_s3.put_object(bucket="b", key="multi.bin", body=b"y")
    await save_multipart_metadata(
        mock_s3,
        "b",
        "multi.bin",
        MultipartMetadata(version=2, part_count=3, wrapped_dek=b"dek", kid="k"),
    )
    # 3) plaintext: no tag, no sidecar
    await mock_s3.put_object(bucket="b", key="plain.txt", body=b"z")

    objects = [{"key": "single.bin"}, {"key": "multi.bin"}, {"key": "plain.txt"}]
    await _annotate_encryption(settings, mock_s3, "b", objects)

    by_key = {o["key"]: o["encrypted"] for o in objects}
    assert by_key["single.bin"] is True
    assert by_key["multi.bin"] is True
    assert by_key["plain.txt"] is False


async def test_listing_paginates_objects(mock_s3, settings, monkeypatch) -> None:
    """The explorer paginates objects (page_size) and HEAD-annotates only the page."""

    class _CtxClient:
        async def __aenter__(self):
            return mock_s3

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(client_mod, "S3Client", lambda *a, **k: _CtxClient())

    for i in range(25):
        await mock_s3.put_object(
            bucket="bkt",
            key=f"d/obj{i:02d}",
            body=b"x",
            metadata={settings.dektag_name: "w"},
        )

    creds = {"AKIAIOSFODNN7EXAMPLE": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}
    page1 = await list_bucket_objects(settings, creds, "bkt", prefix="d/", offset=0, page_size=20)
    assert page1["total_objects"] == 25
    assert len(page1["objects"]) == 20
    assert page1["has_more"] is True
    assert all(o["encrypted"] is True for o in page1["objects"])

    page2 = await list_bucket_objects(settings, creds, "bkt", prefix="d/", offset=20, page_size=20)
    assert len(page2["objects"]) == 5
    assert page2["has_more"] is False
