"""Framed internal-part encryption: stream many small GCM frames per S3 part so
memory stays O(frame) regardless of part size, while staying backward compatible
with legacy single-seal parts already stored in S3."""

import pytest

from s3proxy import crypto

UPLOAD_ID = "u" * 40
PART = 7
F = crypto.FRAME_PLAINTEXT_SIZE


def _encrypt_framed(plaintext: bytes, dek: bytes) -> bytes:
    out = bytearray()
    for i in range(0, max(1, len(plaintext)), F):
        out += crypto.encrypt_frame(plaintext[i : i + F], dek, UPLOAD_ID, PART, i // F)
    return bytes(out)


@pytest.mark.parametrize(
    "size",
    [0, 1, 1024, F - 1, F, F + 1, 2 * F, 3 * F - 17, 5 * F + 123],
)
def test_framed_roundtrip(size):
    dek = crypto.generate_dek()
    plaintext = bytes((i * 31) % 256 for i in range(size)) if size < 100_000 else b"\xab" * size

    ciphertext = _encrypt_framed(plaintext, dek)

    expected_frames = crypto.frame_count(size) if size else 1
    assert len(ciphertext) == size + expected_frames * crypto.ENCRYPTION_OVERHEAD
    assert crypto.framed_ciphertext_size(size) == len(ciphertext)

    assert crypto.decrypt_framed(ciphertext, dek, size) == plaintext


def test_legacy_single_seal_decrypts_via_framed():
    """A part written by the old code (one seal over a >8MB plaintext) must still
    decrypt through the framed reader — this is the restore-compat guarantee."""
    dek = crypto.generate_dek()
    plaintext = b"\x5c" * (3 * F + 99)  # would be 4 frames if framed
    legacy = crypto.encrypt(plaintext, dek, crypto.derive_part_nonce(UPLOAD_ID, PART))

    # Legacy overhead is exactly one frame regardless of size.
    assert len(legacy) - len(plaintext) == crypto.ENCRYPTION_OVERHEAD
    assert crypto.decrypt_framed(legacy, dek, len(plaintext)) == plaintext


def test_ciphertext_frame_byte_sizes_matches_framed():
    sizes = [
        1,
        crypto.FRAME_PLAINTEXT_SIZE,
        crypto.FRAME_PLAINTEXT_SIZE + 1,
        3 * crypto.FRAME_PLAINTEXT_SIZE,
    ]
    for size in sizes:
        ct = crypto.framed_ciphertext_size(size)
        sizes = crypto.ciphertext_frame_byte_sizes(size, ct)
        assert sum(sizes) == ct
        assert all(s >= crypto.ENCRYPTION_OVERHEAD for s in sizes)


def test_frame_nonces_unique():
    nonces = {crypto.derive_frame_nonce(UPLOAD_ID, PART, i) for i in range(1000)}
    assert len(nonces) == 1000
    # And distinct from the legacy part nonce so an upload never reuses one.
    assert crypto.derive_part_nonce(UPLOAD_ID, PART) not in nonces


MB = 1024 * 1024


@pytest.mark.parametrize(
    "content_mb",
    [9, 16, 50, 100, 160, 200, 512, 1024, 4096, 10_000],
)
def test_memory_bounded_part_size_respects_limits(content_mb):
    """For any client part size: never expand beyond the per-client allocation
    range (or S3 part numbers collide), and never create a non-final part below
    S3's 5MB minimum."""
    cl = content_mb * MB
    size = crypto.memory_bounded_part_size(cl)
    parts = -(-cl // size)
    assert parts <= crypto.MAX_INTERNAL_PARTS_PER_CLIENT
    # every part except possibly the last is `size`; the last is the remainder
    last = cl - (parts - 1) * size
    if parts > 1:
        assert last >= crypto.MIN_PART_SIZE
        assert size >= crypto.MIN_PART_SIZE


def test_memory_bounded_part_size_is_small_until_forced_larger():
    """Small/mid client parts stay ~8MB; size grows only when the 20-part cap
    forces it (e.g. barman's 512MB parts -> ~26MB, not 64MB)."""
    assert crypto.memory_bounded_part_size(50 * MB) <= 9 * MB
    assert crypto.memory_bounded_part_size(160 * MB) <= 9 * MB
    barman = crypto.memory_bounded_part_size(512 * MB)
    assert 20 * MB <= barman <= 32 * MB
