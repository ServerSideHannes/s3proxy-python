"""Cryptographic operations for S3Proxy.

Implements AES-256-GCM encryption/decryption and AES-KWP key wrapping.
"""

import hashlib
import secrets
from dataclasses import dataclass

import structlog
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.keywrap import (
    aes_key_unwrap_with_padding,
    aes_key_wrap_with_padding,
)
from structlog import BoundLogger

from .metrics import BYTES_DECRYPTED, BYTES_ENCRYPTED, ENCRYPTION_OPERATIONS

logger: BoundLogger = structlog.get_logger(__name__)

# Constants
NONCE_SIZE = 12  # 96 bits for AES-GCM
TAG_SIZE = 16  # 128 bits authentication tag
ENCRYPTION_OVERHEAD = NONCE_SIZE + TAG_SIZE  # 28 bytes added per encrypted chunk
DEK_SIZE = 32  # 256 bits for AES-256
PART_SIZE = 64 * 1024 * 1024  # 64 MB default part size for internal multipart uploads
MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MB minimum (S3 requirement for all parts except last)
# Streaming threshold: use streaming for parts >= 32MB to avoid OOM
# Elasticsearch uses 51MB parts, so 32MB ensures they stream
# Prevents buffering entire part in memory via request.body()
STREAMING_THRESHOLD = 32 * 1024 * 1024  # 32 MB
# Maximum buffer size: limit memory usage by capping internal part size
# CRITICAL: Native memory (cryptography/aiohttp) not tracked by Python GC
# tracemalloc: 0.08MB Python heap, but 236MB native (crypto/network buffers)
# Smaller buffers (5MB) = more overhead (266MB), larger buffers (8MB) = less overhead (236MB)
# Sweet spot: 8MB balances part count vs per-part overhead
MAX_BUFFER_SIZE = 8 * 1024 * 1024  # 8 MB per internal part


def calculate_optimal_part_size(content_length: int) -> int:
    """Calculate optimal part size to avoid creating parts < 5MB that aren't the final part."""
    # If content fits in default PART_SIZE, check if it needs splitting for memory
    if content_length <= PART_SIZE:
        # Cap at MAX_BUFFER_SIZE to avoid OOM on large uploads
        # Example: 51MB upload splits into 2×25.5MB parts instead of 1×51MB
        if content_length <= MAX_BUFFER_SIZE:
            logger.debug(
                "Content fits in one part - no splitting needed",
                content_length=content_length,
                content_length_mb=f"{content_length / 1024 / 1024:.2f}MB",
                part_size=PART_SIZE,
                part_size_mb=f"{PART_SIZE / 1024 / 1024:.2f}MB",
                decision="no_split",
            )
            return content_length
        else:
            # Split into MAX_BUFFER_SIZE chunks to limit memory usage
            num_parts = (content_length + MAX_BUFFER_SIZE - 1) // MAX_BUFFER_SIZE
            optimal_size = (content_length + num_parts - 1) // num_parts
            logger.info(
                "Splitting to respect MAX_BUFFER_SIZE for memory management",
                content_length=content_length,
                content_length_mb=f"{content_length / 1024 / 1024:.2f}MB",
                max_buffer_size=MAX_BUFFER_SIZE,
                max_buffer_mb=f"{MAX_BUFFER_SIZE / 1024 / 1024:.2f}MB",
                num_parts=num_parts,
                optimal_size=optimal_size,
                optimal_size_mb=f"{optimal_size / 1024 / 1024:.2f}MB",
                decision="split_for_memory",
            )
            return optimal_size

    # Calculate how many parts we'd get with default size
    num_parts = (content_length + PART_SIZE - 1) // PART_SIZE
    remainder = content_length % PART_SIZE

    # If remainder is too small (< 5MB) and there are multiple parts,
    # redistribute content evenly to avoid creating a small non-final part
    if remainder > 0 and remainder < MIN_PART_SIZE and num_parts > 1:
        # Distribute content evenly across fewer parts
        # This ensures all parts are >= MIN_PART_SIZE
        optimal_num_parts = max(1, content_length // PART_SIZE)  # One fewer part
        optimal_size = (content_length + optimal_num_parts - 1) // optimal_num_parts

        logger.info(
            "Detected small remainder - redistributing to avoid EntityTooSmall",
            content_length=content_length,
            content_length_mb=f"{content_length / 1024 / 1024:.2f}MB",
            default_part_size=PART_SIZE,
            default_num_parts=num_parts,
            remainder=remainder,
            remainder_mb=f"{remainder / 1024 / 1024:.2f}MB",
            min_part_size=MIN_PART_SIZE,
            optimal_num_parts=optimal_num_parts,
            optimal_size=optimal_size,
            optimal_size_mb=f"{optimal_size / 1024 / 1024:.2f}MB",
            decision="redistribute_evenly",
        )
        return optimal_size

    # Default size works fine (remainder is >= 5MB or is the last part)
    logger.debug(
        "Default part size is optimal",
        content_length=content_length,
        content_length_mb=f"{content_length / 1024 / 1024:.2f}MB",
        num_parts=num_parts,
        remainder=remainder,
        remainder_mb=f"{remainder / 1024 / 1024:.2f}MB" if remainder > 0 else "0MB",
        part_size=PART_SIZE,
        decision="use_default",
    )
    return PART_SIZE


@dataclass(slots=True)
class EncryptedData:
    """Container for encrypted data and metadata."""

    ciphertext: bytes  # nonce || ciphertext || tag
    wrapped_dek: bytes  # AES-KWP wrapped DEK


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
    """Wrap DEK using AES-KWP (Key Wrap with Padding)."""
    try:
        wrapped = aes_key_wrap_with_padding(kek, dek)
        logger.debug(
            "DEK wrapped successfully",
            dek_size=len(dek),
            wrapped_size=len(wrapped),
        )
        return wrapped
    except Exception as e:
        logger.error(
            "Failed to wrap DEK",
            dek_size=len(dek),
            error=str(e),
            error_type=type(e).__name__,
        )
        raise


def unwrap_key(wrapped_dek: bytes, kek: bytes) -> bytes:
    """Unwrap DEK using AES-KWP."""
    try:
        dek = aes_key_unwrap_with_padding(kek, wrapped_dek)
        logger.debug(
            "DEK unwrapped successfully",
            wrapped_size=len(wrapped_dek),
            dek_size=len(dek),
        )
        return dek
    except Exception as e:
        logger.error(
            "Failed to unwrap DEK - possible key mismatch or corruption",
            wrapped_size=len(wrapped_dek),
            error=str(e),
            error_type=type(e).__name__,
        )
        raise


def encrypt(plaintext: bytes, dek: bytes, nonce: bytes | None = None) -> bytes:
    """Encrypt data using AES-256-GCM. Returns nonce (12) || ciphertext || tag (16)."""
    if nonce is None:
        nonce = generate_nonce()

    try:
        aesgcm = AESGCM(dek)
        ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext, None)
        result = nonce + ciphertext_with_tag

        # Track metrics
        ENCRYPTION_OPERATIONS.labels(operation="encrypt").inc()
        BYTES_ENCRYPTED.inc(len(plaintext))

        logger.debug(
            "Data encrypted",
            plaintext_size=len(plaintext),
            ciphertext_size=len(result),
            nonce_prefix=nonce[:4].hex(),
        )
        return result
    except Exception as e:
        logger.error(
            "Encryption failed",
            plaintext_size=len(plaintext),
            error=str(e),
            error_type=type(e).__name__,
        )
        raise


def decrypt(ciphertext: bytes, dek: bytes) -> bytes:
    """Decrypt data using AES-256-GCM. Expects nonce (12) || ciphertext || tag (16)."""
    if len(ciphertext) < ENCRYPTION_OVERHEAD:
        logger.error(
            "Ciphertext too short for decryption",
            ciphertext_size=len(ciphertext),
            minimum_required=ENCRYPTION_OVERHEAD,
        )
        raise ValueError(
            f"Ciphertext too short: {len(ciphertext)} bytes, minimum {ENCRYPTION_OVERHEAD} required"
        )

    nonce = ciphertext[:NONCE_SIZE]
    ct_with_tag = ciphertext[NONCE_SIZE:]

    try:
        aesgcm = AESGCM(dek)
        plaintext = aesgcm.decrypt(nonce, ct_with_tag, None)

        # Track metrics
        ENCRYPTION_OPERATIONS.labels(operation="decrypt").inc()
        BYTES_DECRYPTED.inc(len(plaintext))

        logger.debug(
            "Data decrypted",
            ciphertext_size=len(ciphertext),
            plaintext_size=len(plaintext),
            nonce_prefix=nonce[:4].hex(),
        )
        return plaintext
    except Exception as e:
        logger.error(
            "Decryption failed - possible corruption or wrong key",
            ciphertext_size=len(ciphertext),
            nonce_prefix=nonce[:4].hex(),
            error=str(e),
            error_type=type(e).__name__,
        )
        raise


def encrypt_object(plaintext: bytes, kek: bytes) -> EncryptedData:
    """Encrypt an object with a new DEK, wrapped with KEK."""
    logger.debug(
        "Encrypting object",
        plaintext_size=len(plaintext),
        plaintext_size_mb=f"{len(plaintext) / 1024 / 1024:.2f}MB",
    )
    dek = generate_dek()
    ciphertext = encrypt(plaintext, dek)
    wrapped_dek = wrap_key(dek, kek)

    logger.debug(
        "Object encrypted successfully",
        plaintext_size=len(plaintext),
        ciphertext_size=len(ciphertext),
        wrapped_dek_size=len(wrapped_dek),
    )
    return EncryptedData(ciphertext=ciphertext, wrapped_dek=wrapped_dek)


def decrypt_object(ciphertext: bytes, wrapped_dek: bytes, kek: bytes) -> bytes:
    """Decrypt an object using wrapped DEK."""
    logger.debug(
        "Decrypting object",
        ciphertext_size=len(ciphertext),
        ciphertext_size_mb=f"{len(ciphertext) / 1024 / 1024:.2f}MB",
        wrapped_dek_size=len(wrapped_dek),
    )
    dek = unwrap_key(wrapped_dek, kek)
    plaintext = decrypt(ciphertext, dek)
    logger.debug(
        "Object decrypted successfully",
        ciphertext_size=len(ciphertext),
        plaintext_size=len(plaintext),
    )
    return plaintext


def encrypt_part(plaintext: bytes, dek: bytes, upload_id: str, part_number: int) -> bytes:
    """Encrypt a multipart upload part with deterministic nonce."""
    nonce = derive_part_nonce(upload_id, part_number)
    logger.debug(
        "Encrypting part",
        upload_id=upload_id[:20] + "..." if len(upload_id) > 20 else upload_id,
        part_number=part_number,
        plaintext_size=len(plaintext),
        nonce_prefix=nonce[:4].hex(),
    )
    return encrypt(plaintext, dek, nonce)


def decrypt_part(ciphertext: bytes, dek: bytes, upload_id: str, part_number: int) -> bytes:
    """Decrypt a multipart upload part."""
    # The nonce is already embedded in the ciphertext, but we can verify it
    expected_nonce = derive_part_nonce(upload_id, part_number)
    actual_nonce = ciphertext[:NONCE_SIZE]

    if expected_nonce != actual_nonce:
        logger.error(
            "Nonce mismatch during part decryption",
            upload_id=upload_id[:20] + "..." if len(upload_id) > 20 else upload_id,
            part_number=part_number,
            expected_nonce_prefix=expected_nonce[:4].hex(),
            actual_nonce_prefix=actual_nonce[:4].hex(),
        )
        raise ValueError(f"Nonce mismatch for part {part_number}")

    logger.debug(
        "Decrypting part",
        upload_id=upload_id[:20] + "..." if len(upload_id) > 20 else upload_id,
        part_number=part_number,
        ciphertext_size=len(ciphertext),
    )
    return decrypt(ciphertext, dek)


def ciphertext_size(plaintext_size: int) -> int:
    """Calculate ciphertext size from plaintext size.

    ciphertext = nonce (12) + plaintext + tag (16)
    """
    return plaintext_size + NONCE_SIZE + TAG_SIZE


def plaintext_size(ciphertext_size: int) -> int:
    """Calculate plaintext size from ciphertext size."""
    return ciphertext_size - NONCE_SIZE - TAG_SIZE
