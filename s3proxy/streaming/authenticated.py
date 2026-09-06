"""Bounded legacy GCM reads: authenticate fully before exposing plaintext."""

import asyncio
import tempfile

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from .. import crypto
from ..errors import S3Error


async def decrypt_to_file(body, dek):
    spool = tempfile.SpooledTemporaryFile(max_size=crypto.MAX_BUFFER_SIZE)  # noqa: SIM115 -- response owns it
    # Roll large plaintext to disk without holding a second full plaintext copy.
    pending = bytearray()
    decryptor = None
    length = 0
    try:
        async with body:
            while chunk := await body.read(1024 * 1024):
                pending.extend(chunk)
                if decryptor is None and len(pending) >= crypto.NONCE_SIZE:
                    nonce = bytes(pending[: crypto.NONCE_SIZE])
                    del pending[: crypto.NONCE_SIZE]
                    decryptor = Cipher(algorithms.AES(dek), modes.GCM(nonce)).decryptor()
                if decryptor and len(pending) > crypto.TAG_SIZE:
                    data = decryptor.update(bytes(pending[: -crypto.TAG_SIZE]))
                    del pending[: -crypto.TAG_SIZE]
                    length += len(data)
                    await asyncio.to_thread(spool.write, data)
        if decryptor is None or len(pending) != crypto.TAG_SIZE:
            raise S3Error.internal_error("Truncated encrypted object")
        decryptor.finalize_with_tag(bytes(pending))
        return spool, length
    except BaseException:
        spool.close()
        raise


async def file_range(spool, start, end):
    try:
        await asyncio.to_thread(spool.seek, start)
        remaining = end - start + 1
        while remaining > 0:
            chunk = await asyncio.to_thread(spool.read, min(1024 * 1024, remaining))
            if not chunk:
                raise S3Error.internal_error("Truncated plaintext spool")
            remaining -= len(chunk)
            yield chunk
    finally:
        spool.close()
