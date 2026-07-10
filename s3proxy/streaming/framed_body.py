"""Streaming upload body that seals AES-GCM frames on the fly.

Upload paths used to accumulate a whole internal part of ciphertext before
handing it to the backend client, so peak memory tracked the internal part
size (hundreds of MB for multi-GB client parts). FramedStreamBody instead
yields each sealed frame as encryption produces it: peak memory is O(frame),
independent of part size.

The body reports the exact framed ciphertext size via __len__, so botocore
sends a normal Content-Length request (aiohttp only falls back to chunked
transfer encoding when no length is known). It tracks a running MD5 of the
ciphertext for backend ETag verification (see client.s3.verify_backend_etag).

Single-shot: botocore cannot replay it on its internal retries (no seek).
Callers own retry semantics — the copy pump rebuilds the source stream and
retries the internal part itself; the upload path surfaces the failure so the
client re-sends its part.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

from .. import crypto

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class SourceExhaustedError(Exception):
    """The plaintext source ended before yielding the promised bytes."""


# Sealed frames are handed to the transport in slices this big, so the consumer
# never pins a whole 8MB frame while the next one is being produced (that held
# frame was a full quarter of the measured peak).
FRAME_YIELD_SLICE = 1024 * 1024


class FramedStreamBody:
    """Async-iterable request body of sealed AES-GCM frames for one internal part."""

    def __init__(
        self,
        reader: Any,
        plaintext_size: int,
        dek: bytes,
        upload_id: str,
        part_number: int,
        *,
        plaintext_hashes: tuple[Any, ...] = (),
    ) -> None:
        if plaintext_size <= 0:
            raise ValueError("plaintext_size must be positive")
        self._reader = reader
        self._plaintext_size = plaintext_size
        self._dek = dek
        self._upload_id = upload_id
        self._part_number = part_number
        self._plaintext_hashes = plaintext_hashes
        self._ciphertext_md5 = hashlib.md5(usedforsecurity=False)
        self._consumed = False
        self.bytes_read = 0
        self.short_read = False

    def __len__(self) -> int:
        return crypto.framed_ciphertext_size(self._plaintext_size)

    def read(self, *args: Any, **kwargs: Any) -> bytes:
        # Present only so botocore's blob param validation accepts the body
        # (bytes-like or has `read`). Nothing may consume the body this way:
        # payload signing and flexible checksums are disabled on the backend
        # client, and aiohttp streams via __aiter__.
        raise NotImplementedError("FramedStreamBody is consumed via async iteration")

    def ciphertext_md5_hexdigest(self) -> str:
        return self._ciphertext_md5.hexdigest()

    def __aiter__(self) -> AsyncIterator[bytes]:
        if self._consumed:
            raise RuntimeError("FramedStreamBody is single-shot and was already consumed")
        self._consumed = True
        return self._frames()

    async def _frames(self) -> AsyncIterator[bytes]:
        remaining = self._plaintext_size
        frame_index = 0
        while remaining > 0:
            want = min(crypto.FRAME_PLAINTEXT_SIZE, remaining)
            frame_pt = await self._reader.read(want)
            self.bytes_read += len(frame_pt)
            if len(frame_pt) < want:
                self.short_read = True
                raise SourceExhaustedError(
                    f"source ended after {self.bytes_read} of {self._plaintext_size} "
                    f"plaintext bytes for internal part {self._part_number}"
                )
            for h in self._plaintext_hashes:
                h.update(frame_pt)
            remaining -= want
            frame = crypto.encrypt_frame(
                frame_pt, self._dek, self._upload_id, self._part_number, frame_index
            )
            del frame_pt
            self._ciphertext_md5.update(frame)
            frame_index += 1
            view = memoryview(frame)
            for offset in range(0, len(frame), FRAME_YIELD_SLICE):
                yield bytes(view[offset : offset + FRAME_YIELD_SLICE])
            # Drop the frame before the next read so the generator never holds
            # two frames at once (keeps the peak at ~2 frames, not 4).
            view.release()
            del frame
