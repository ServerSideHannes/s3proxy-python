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
    for size in [1, crypto.FRAME_PLAINTEXT_SIZE, crypto.FRAME_PLAINTEXT_SIZE + 1, 3 * crypto.FRAME_PLAINTEXT_SIZE]:
        ct = crypto.framed_ciphertext_size(size)
        sizes = crypto.ciphertext_frame_byte_sizes(size, ct)
        assert sum(sizes) == ct
        assert all(s >= crypto.ENCRYPTION_OVERHEAD for s in sizes)


def test_frame_nonces_unique():
    nonces = {crypto.derive_frame_nonce(UPLOAD_ID, PART, i) for i in range(1000)}
    assert len(nonces) == 1000
    # And distinct from the legacy part nonce so an upload never reuses one.
    assert crypto.derive_part_nonce(UPLOAD_ID, PART) not in nonces
