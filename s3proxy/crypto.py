"""Cryptographic operations for S3Proxy.

Implements AES-256-GCM encryption/decryption and AES-KWP key wrapping.
"""

import hashlib
import secrets
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.keywrap import (
    aes_key_unwrap_with_padding,
    aes_key_wrap_with_padding,
)

# Constants
NONCE_SIZE = 12  # 96 bits for AES-GCM
TAG_SIZE = 16    # 128 bits authentication tag
DEK_SIZE = 32    # 256 bits for AES-256
PART_SIZE = 16 * 1024 * 1024  # 16 MB default part size


@dataclass(slots=True)
class EncryptedData:
    """Container for encrypted data and metadata."""
    ciphertext: bytes  # nonce || ciphertext || tag
    wrapped_dek: bytes  # AES-KWP wrapped DEK


@dataclass(slots=True)
class DecryptedData:
    """Container for decrypted data."""
    plaintext: bytes


def generate_dek() -> bytes:
    """Generate a random 32-byte Data Encryption Key."""
    return secrets.token_bytes(DEK_SIZE)


def generate_nonce() -> bytes:
    """Generate a random 12-byte nonce."""
    return secrets.token_bytes(NONCE_SIZE)


def derive_part_nonce(upload_id: str, part_number: int) -> bytes:
    """Derive a deterministic nonce for a multipart upload part.

    nonce = SHA256(uploadID || partNumber)[:12]
    """
    data = f"{upload_id}{part_number}".encode()
    return hashlib.sha256(data).digest()[:NONCE_SIZE]


def wrap_key(dek: bytes, kek: bytes) -> bytes:
    """Wrap DEK using AES-KWP (Key Wrap with Padding).

    Args:
        dek: 32-byte Data Encryption Key
        kek: 32-byte Key Encryption Key

    Returns:
        Wrapped DEK (40 bytes for 32-byte input)
    """
    return aes_key_wrap_with_padding(kek, dek)


def unwrap_key(wrapped_dek: bytes, kek: bytes) -> bytes:
    """Unwrap DEK using AES-KWP.

    Args:
        wrapped_dek: Wrapped Data Encryption Key
        kek: 32-byte Key Encryption Key

    Returns:
        32-byte Data Encryption Key
    """
    return aes_key_unwrap_with_padding(kek, wrapped_dek)


def encrypt(plaintext: bytes, dek: bytes, nonce: bytes | None = None) -> bytes:
    """Encrypt data using AES-256-GCM.

    Args:
        plaintext: Data to encrypt
        dek: 32-byte Data Encryption Key
        nonce: Optional 12-byte nonce (random if not provided)

    Returns:
        nonce (12) || ciphertext || tag (16)
    """
    if nonce is None:
        nonce = generate_nonce()

    aesgcm = AESGCM(dek)
    ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext, None)

    return nonce + ciphertext_with_tag


def decrypt(ciphertext: bytes, dek: bytes) -> bytes:
    """Decrypt data using AES-256-GCM.

    Args:
        ciphertext: nonce (12) || ciphertext || tag (16)
        dek: 32-byte Data Encryption Key

    Returns:
        Decrypted plaintext
    """
    nonce = ciphertext[:NONCE_SIZE]
    ct_with_tag = ciphertext[NONCE_SIZE:]

    aesgcm = AESGCM(dek)
    return aesgcm.decrypt(nonce, ct_with_tag, None)


def encrypt_object(plaintext: bytes, kek: bytes) -> EncryptedData:
    """Encrypt an object with a new DEK, wrapped with KEK.

    Args:
        plaintext: Data to encrypt
        kek: 32-byte Key Encryption Key

    Returns:
        EncryptedData with ciphertext and wrapped DEK
    """
    dek = generate_dek()
    ciphertext = encrypt(plaintext, dek)
    wrapped_dek = wrap_key(dek, kek)

    return EncryptedData(ciphertext=ciphertext, wrapped_dek=wrapped_dek)


def decrypt_object(ciphertext: bytes, wrapped_dek: bytes, kek: bytes) -> bytes:
    """Decrypt an object using wrapped DEK.

    Args:
        ciphertext: nonce || ciphertext || tag
        wrapped_dek: AES-KWP wrapped DEK
        kek: 32-byte Key Encryption Key

    Returns:
        Decrypted plaintext
    """
    dek = unwrap_key(wrapped_dek, kek)
    return decrypt(ciphertext, dek)


def encrypt_part(plaintext: bytes, dek: bytes, upload_id: str, part_number: int) -> bytes:
    """Encrypt a multipart upload part with deterministic nonce.

    Args:
        plaintext: Part data to encrypt
        dek: 32-byte Data Encryption Key (shared across all parts)
        upload_id: S3 upload ID
        part_number: Part number (1-indexed)

    Returns:
        nonce (12) || ciphertext || tag (16)
    """
    nonce = derive_part_nonce(upload_id, part_number)
    return encrypt(plaintext, dek, nonce)


def decrypt_part(ciphertext: bytes, dek: bytes, upload_id: str, part_number: int) -> bytes:
    """Decrypt a multipart upload part.

    Args:
        ciphertext: nonce || ciphertext || tag
        dek: 32-byte Data Encryption Key
        upload_id: S3 upload ID
        part_number: Part number (1-indexed)

    Returns:
        Decrypted plaintext
    """
    # The nonce is already embedded in the ciphertext, but we can verify it
    expected_nonce = derive_part_nonce(upload_id, part_number)
    actual_nonce = ciphertext[:NONCE_SIZE]

    if expected_nonce != actual_nonce:
        raise ValueError(f"Nonce mismatch for part {part_number}")

    return decrypt(ciphertext, dek)


def ciphertext_size(plaintext_size: int) -> int:
    """Calculate ciphertext size from plaintext size.

    ciphertext = nonce (12) + plaintext + tag (16)
    """
    return plaintext_size + NONCE_SIZE + TAG_SIZE


def plaintext_size(ciphertext_size: int) -> int:
    """Calculate plaintext size from ciphertext size."""
    return ciphertext_size - NONCE_SIZE - TAG_SIZE
