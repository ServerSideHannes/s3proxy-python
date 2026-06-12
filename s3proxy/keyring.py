"""Per-access-key encryption key resolution.

Each AWS access key has its own KEK (derived from a per-credential secret). The
access key that wrapped an object's DEK is stored alongside the object as its
``kid``, so decryption is always driven by the stored kid - never re-derived
from the request. A verified access key with no configured KEK is rejected.
"""

from __future__ import annotations

import hashlib


class UnknownKidError(Exception):
    """An object's wrapping key (kid) is not configured.

    Terminal, not transient: the object cannot be decrypted until the key for
    this kid is configured, so callers surface a non-retryable error rather than
    a 5xx that backup clients would retry-storm against.
    """

    def __init__(self, kid: str):
        self.kid = kid
        super().__init__(f"No key configured for kid {kid!r}")


def derive_kek(secret: str) -> bytes:
    """Derive a 32-byte KEK from a per-credential secret (SHA256)."""
    return hashlib.sha256(secret.encode()).digest()


class KeyRing:
    """Immutable resolver from access key -> KEK, and kid -> KEK.

    The kid is the access key that wrapped an object. Reads are lock-free; the
    ring is built once at startup and never mutated.
    """

    __slots__ = ("_keys",)

    def __init__(self, keys: dict[str, bytes]):
        # keys: access_key -> KEK.
        self._keys = dict(keys)

    def key_for(self, access_key: str) -> tuple[str, bytes]:
        """Resolve the kid + KEK to encrypt an object written by this access key.

        Raises KeyError if the access key has no configured KEK.
        """
        try:
            return access_key, self._keys[access_key]
        except KeyError as e:
            raise KeyError(f"No encryption key configured for access key {access_key!r}") from e

    def key_by_id(self, kid: str) -> bytes:
        """Resolve the KEK that wrapped an object's DEK, by its stored kid.

        Raises KeyError for an empty or unknown kid - we cannot guess the
        wrapping key.
        """
        try:
            return self._keys[kid]
        except KeyError as e:
            raise UnknownKidError(kid) from e
