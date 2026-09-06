"""Read contiguous ciphertext ranges while authenticating one frame at a time."""

import asyncio
import contextlib

from .. import crypto


async def read_frames(client, bucket, key, frames, *, if_match=None):
    # Frames are (ciphertext_offset, ciphertext_size, plaintext_slice_start, end).
    # The window limits retry scope, not buffering: only one frame is accumulated.
    index = 0
    while index < len(frames):
        end_index = index + 1
        end = frames[index][0] + frames[index][1]
        while end_index < len(frames):
            offset, size, _, _ = frames[end_index]
            if offset != end or offset + size - frames[index][0] > 64 * 1024**2:
                break
            end += size
            end_index += 1
        from ..handlers.base import (
            SOURCE_READ_ATTEMPTS,
            SOURCE_READ_BACKOFF_SEC,
            is_retryable_source_error,
        )

        attempt = 0
        while index < end_index:
            response = None
            try:
                response = await client.get_object(
                    bucket,
                    key,
                    f"bytes={frames[index][0]}-{end - 1}",
                    **({"if_match": if_match} if if_match else {}),
                )
                body = response["Body"]
                async with body:
                    while index < end_index:
                        _, size, start, stop = frames[index]
                        ciphertext = bytearray(size)
                        received = 0
                        while received < size:
                            chunk = await body.read(min(1024**2, size - received))
                            if not chunk:
                                raise EOFError("Truncated ciphertext range")
                            ciphertext[received : received + len(chunk)] = chunk
                            received += len(chunk)
                        yield ciphertext, start, stop
                        index += 1
                        attempt = 0
            except Exception as error:
                attempt += 1
                if attempt >= SOURCE_READ_ATTEMPTS or not (
                    isinstance(error, EOFError) or is_retryable_source_error(error)
                ):
                    raise
                await asyncio.sleep(SOURCE_READ_BACKOFF_SEC * 2 ** (attempt - 1))


async def plaintext_frames(client, bucket, key, meta, dek, start=None, end=None, *, if_match=None):
    frames = []
    pt_offset = 0
    ct_offset = 0
    for part in sorted(meta.parts, key=lambda p: p.part_number):
        segments = part.internal_parts or [part]
        for segment in segments:
            for size in crypto.ciphertext_frame_byte_sizes(
                segment.plaintext_size, segment.ciphertext_size
            ):
                plaintext_size = size - crypto.ENCRYPTION_OVERHEAD
                if start is None or (pt_offset + plaintext_size > start and pt_offset <= end):
                    left = max(0, start - pt_offset) if start is not None else 0
                    right = (
                        min(plaintext_size, end - pt_offset + 1)
                        if end is not None
                        else plaintext_size
                    )
                    frames.append((ct_offset, size, left, right))
                pt_offset += plaintext_size
                ct_offset += size
    # Old objects may contain a single GCM seal larger than the modern frame.
    # Authenticate those seals to a bounded spool before releasing any plaintext.
    index = 0
    while index < len(frames):
        offset, size, left, right = frames[index]
        if size > (crypto.FRAME_PLAINTEXT_SIZE + crypto.ENCRYPTION_OVERHEAD):
            from .authenticated import decrypt_to_file, file_range

            response = await client.get_object(
                bucket,
                key,
                f"bytes={offset}-{offset + size - 1}",
                **({"if_match": if_match} if if_match else {}),
            )
            spool, _ = await decrypt_to_file(response["Body"], dek)
            try:
                async with contextlib.aclosing(file_range(spool, left, right - 1)) as stream:
                    async for chunk in stream:
                        yield chunk
            finally:
                spool.close()
            index += 1
            continue
        stop = index + 1
        while stop < len(frames) and frames[stop][1] <= (
            crypto.FRAME_PLAINTEXT_SIZE + crypto.ENCRYPTION_OVERHEAD
        ):
            stop += 1
        async with contextlib.aclosing(
            read_frames(client, bucket, key, frames[index:stop], if_match=if_match)
        ) as reader:
            async for ciphertext, left, right in reader:
                plaintext = crypto.decrypt(ciphertext, dek)
                for offset in range(left, right, 1024**2):
                    yield plaintext[offset : min(offset + 1024**2, right)]
                del plaintext, ciphertext
        index = stop
