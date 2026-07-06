"""CopyObject must use reserve_copy_memory (copy clamp), not upload clamp."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from s3proxy import concurrency, crypto
from s3proxy.handlers.objects.misc import MiscObjectMixin

MB = 1024 * 1024


class _Mgr:
    async def create_upload(self, *a, **k):
        return None

    async def get_upload(self, *a, **k):
        return None


def _handler():
    h = MiscObjectMixin.__new__(MiscObjectMixin)
    h.multipart_manager = _Mgr()
    h.keyring = MagicMock()
    h.keyring.key_for.return_value = ("kid", b"k" * 32)
    h.settings = MagicMock()
    h.settings.dektag_name = "x-amz-meta-dek"
    h.settings.kidtag_name = "x-amz-meta-kid"
    return h


@pytest.mark.asyncio
async def test_copy_encrypted_streaming_uses_reserve_copy_memory():
    """Large CopyObject must not go through upload-style routine clamp."""
    h = _handler()
    pt_size = 512 * MB
    acquired: list[int] = []

    @asynccontextmanager
    async def spy_reserve(needed: int):
        acquired.append(needed)
        yield

    meta = type("M", (), {"total_plaintext_size": pt_size})()
    head = {"Metadata": {}, "ContentLength": pt_size, "ContentType": "application/octet-stream"}

    with (
        patch.object(concurrency, "reserve_copy_memory", side_effect=spy_reserve),
        patch.object(h, "_copy_encrypted_inner", new_callable=AsyncMock) as mock_inner,
    ):
        mock_inner.return_value = MagicMock(status_code=200)
        await h._copy_encrypted(
            MagicMock(),
            "b",
            "dst",
            None,
            "b",
            "src",
            head,
            None,
            meta,
            "COPY",
            None,
        )

    expected = crypto.copy_pipeline_peak(pt_size)
    assert acquired == [expected]
    assert expected > crypto.governor_memory_footprint(pt_size)
