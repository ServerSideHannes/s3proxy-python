"""Tests for crypto module."""

import hashlib

import pytest

from s3proxy import crypto


class TestKeyGeneration:
    """Test key and nonce generation."""

    def test_generate_dek(self):
        """Test DEK generation produces 32 bytes."""
        dek = crypto.generate_dek()
        assert len(dek) == 32
        assert isinstance(dek, bytes)

    def test_generate_dek_unique(self):
        """Test DEK generation produces unique keys."""
        dek1 = crypto.generate_dek()
        dek2 = crypto.generate_dek()
        assert dek1 != dek2

    def test_generate_nonce(self):
        """Test nonce generation produces 12 bytes."""
        nonce = crypto.generate_nonce()
        assert len(nonce) == 12
        assert isinstance(nonce, bytes)

    def test_derive_part_nonce(self):
        """Test deterministic part nonce derivation."""
        upload_id = "test-upload-123"
        part_number = 1

        nonce1 = crypto.derive_part_nonce(upload_id, part_number)
        nonce2 = crypto.derive_part_nonce(upload_id, part_number)

        assert nonce1 == nonce2
        assert len(nonce1) == 12

    def test_derive_part_nonce_different_parts(self):
        """Test different parts get different nonces."""
        upload_id = "test-upload-123"

        nonce1 = crypto.derive_part_nonce(upload_id, 1)
        nonce2 = crypto.derive_part_nonce(upload_id, 2)

        assert nonce1 != nonce2


class TestKeyWrapping:
    """Test AES-KWP key wrapping."""

    def test_wrap_unwrap_key(self):
        """Test key wrap and unwrap roundtrip."""
        kek = hashlib.sha256(b"test-kek").digest()
        dek = crypto.generate_dek()

        wrapped = crypto.wrap_key(dek, kek)
        unwrapped = crypto.unwrap_key(wrapped, kek)

        assert unwrapped == dek

    def test_wrapped_key_size(self):
        """Test wrapped key is larger than original."""
        kek = hashlib.sha256(b"test-kek").digest()
        dek = crypto.generate_dek()

        wrapped = crypto.wrap_key(dek, kek)

        # AES-KWP adds 8 bytes of padding
        assert len(wrapped) == 40


class TestEncryption:
    """Test AES-GCM encryption/decryption."""

    def test_encrypt_decrypt(self):
        """Test basic encryption/decryption roundtrip."""
        dek = crypto.generate_dek()
        plaintext = b"Hello, World!"

        ciphertext = crypto.encrypt(plaintext, dek)
        decrypted = crypto.decrypt(ciphertext, dek)

        assert decrypted == plaintext

    def test_encrypt_with_nonce(self):
        """Test encryption with explicit nonce."""
        dek = crypto.generate_dek()
        plaintext = b"Test data"
        nonce = crypto.generate_nonce()

        ciphertext = crypto.encrypt(plaintext, dek, nonce)

        # Verify nonce is embedded
        assert ciphertext[:12] == nonce

        # Verify decryption works
        decrypted = crypto.decrypt(ciphertext, dek)
        assert decrypted == plaintext

    def test_ciphertext_format(self):
        """Test ciphertext format is nonce || ct || tag."""
        dek = crypto.generate_dek()
        plaintext = b"Test"

        ciphertext = crypto.encrypt(plaintext, dek)

        # Format: nonce (12) + ciphertext (len(plaintext)) + tag (16)
        expected_len = 12 + len(plaintext) + 16
        assert len(ciphertext) == expected_len

    def test_encrypt_empty(self):
        """Test encryption of empty data."""
        dek = crypto.generate_dek()
        plaintext = b""

        ciphertext = crypto.encrypt(plaintext, dek)
        decrypted = crypto.decrypt(ciphertext, dek)

        assert decrypted == plaintext

    def test_encrypt_large(self):
        """Test encryption of large data."""
        dek = crypto.generate_dek()
        plaintext = b"x" * (1024 * 1024)  # 1 MB

        ciphertext = crypto.encrypt(plaintext, dek)
        decrypted = crypto.decrypt(ciphertext, dek)

        assert decrypted == plaintext


class TestObjectEncryption:
    """Test full object encryption with key wrapping."""

    def test_encrypt_decrypt_object(self):
        """Test object encryption roundtrip."""
        kek = hashlib.sha256(b"test-kek").digest()
        plaintext = b"Secret data"

        encrypted = crypto.encrypt_object(plaintext, kek)
        decrypted = crypto.decrypt_object(
            encrypted.ciphertext, encrypted.wrapped_dek, kek
        )

        assert decrypted == plaintext

    def test_encrypted_data_contains_wrapped_dek(self):
        """Test encrypted data includes wrapped DEK."""
        kek = hashlib.sha256(b"test-kek").digest()
        plaintext = b"Test"

        encrypted = crypto.encrypt_object(plaintext, kek)

        assert len(encrypted.wrapped_dek) == 40
        assert len(encrypted.ciphertext) > len(plaintext)


class TestPartEncryption:
    """Test multipart upload part encryption."""

    def test_encrypt_decrypt_part(self):
        """Test part encryption roundtrip."""
        dek = crypto.generate_dek()
        upload_id = "upload-123"
        part_number = 1
        plaintext = b"Part data"

        ciphertext = crypto.encrypt_part(plaintext, dek, upload_id, part_number)
        decrypted = crypto.decrypt_part(ciphertext, dek, upload_id, part_number)

        assert decrypted == plaintext

    def test_part_nonce_verification(self):
        """Test part decryption verifies nonce."""
        dek = crypto.generate_dek()
        upload_id = "upload-123"
        plaintext = b"Part data"

        ciphertext = crypto.encrypt_part(plaintext, dek, upload_id, 1)

        # Trying to decrypt with wrong part number should fail
        with pytest.raises(ValueError, match="Nonce mismatch"):
            crypto.decrypt_part(ciphertext, dek, upload_id, 2)


class TestSizeCalculations:
    """Test size calculation functions."""

    def test_ciphertext_size(self):
        """Test ciphertext size calculation."""
        plaintext_size = 100
        expected = 100 + 12 + 16  # plaintext + nonce + tag

        assert crypto.ciphertext_size(plaintext_size) == expected

    def test_plaintext_size(self):
        """Test plaintext size calculation."""
        ciphertext_size = 128
        expected = 128 - 12 - 16  # ciphertext - nonce - tag

        assert crypto.plaintext_size(ciphertext_size) == expected

    def test_size_roundtrip(self):
        """Test size calculation roundtrip."""
        original = 1000

        ct_size = crypto.ciphertext_size(original)
        pt_size = crypto.plaintext_size(ct_size)

        assert pt_size == original
