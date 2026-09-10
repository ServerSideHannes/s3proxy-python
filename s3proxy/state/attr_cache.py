"""Process-local cache of plaintext attributes for completed objects.

Multipart objects can't carry their plaintext size in backend user metadata
(user metadata is fixed at CreateMultipartUpload, before the final size is
known), so list pages must resolve it from the .meta sidecar. This cache
remembers the resolved (size, etag) per (bucket, key, backend_etag) so
repeated listings — e.g. a backup client walking the same directories on
every sync pass — skip the per-object round-trips. Keying by backend ETag
scopes an entry to one object version: overwriting the key changes the
backend ETag, which invalidates the cached attributes without coordination.
"""

import asyncio
import hashlib
from collections import OrderedDict
from contextlib import asynccontextmanager

# ~100k entries of (bucket, key, etag) -> (size, etag) stays in the tens of
# MB even with long backup keys — well inside the pod memory limit.
_DEFAULT_MAXSIZE = 100_000


def synthetic_multipart_etag(plaintext_size: int) -> str:
    """ETag reported for multipart-encrypted objects.

    Must stay in sync with what CompleteMultipartUpload and HEAD return for
    these objects, so listings, HEADs, and upload responses all agree.
    """
    return hashlib.md5(str(plaintext_size).encode(), usedforsecurity=False).hexdigest()


class PlaintextAttrCache:
    def __init__(self, maxsize: int = _DEFAULT_MAXSIZE) -> None:
        self._maxsize = maxsize
        self._locks = {}
        self._entries: OrderedDict[tuple[str, str, str], tuple[int, str]] = OrderedDict()

    @asynccontextmanager
    async def coalesce(self, bucket, key, backend_etag):
        cache_key = (bucket, key, backend_etag)
        entry = self._locks.setdefault(cache_key, [asyncio.Lock(), 0])
        entry[1] += 1
        try:
            async with entry[0]:
                yield
        finally:
            entry[1] -= 1
            if entry[1] == 0:
                del self._locks[cache_key]

    def get(self, bucket: str, key: str, backend_etag: str) -> tuple[int, str] | None:
        if not backend_etag:
            return None
        entry_key = (bucket, key, backend_etag)
        entry = self._entries.get(entry_key)
        if entry is not None:
            self._entries.move_to_end(entry_key)
        return entry

    def put(self, bucket: str, key: str, backend_etag: str, size: int, etag: str) -> None:
        if not backend_etag:
            return
        entry_key = (bucket, key, backend_etag)
        self._entries[entry_key] = (size, etag)
        self._entries.move_to_end(entry_key)
        while len(self._entries) > self._maxsize:
            self._entries.popitem(last=False)

    def clear(self) -> None:
        self._entries.clear()

    def __len__(self) -> int:
        return len(self._entries)


plaintext_attr_cache = PlaintextAttrCache()
