"""Multipart upload state manager."""

import structlog
from structlog.stdlib import BoundLogger

from .models import (
    MultipartUploadState,
    PartMetadata,
    StateMissingError,
)
from .serialization import deserialize_upload_state, serialize_upload_state
from .storage import StateStore

logger: BoundLogger = structlog.get_logger(__name__)

# Maximum internal parts per client part (for range allocation)
MAX_INTERNAL_PARTS_PER_CLIENT = 20


class MultipartStateManager:
    """Manages multipart upload state using pluggable storage backend.

    Uses the Strategy Pattern - storage backend is injected at construction
    or set later via set_store(). Supports Redis (HA) or in-memory (single-instance).
    """

    def __init__(self, store: StateStore | None = None, ttl_seconds: int = 86400) -> None:
        """Initialize state manager.

        Args:
            store: Storage backend. If None, uses MemoryStateStore (can be changed later)
            ttl_seconds: TTL for upload state (default 24 hours)
        """
        from .storage import MemoryStateStore

        self._store = store if store is not None else MemoryStateStore()
        self._ttl = ttl_seconds

    def set_store(self, store: StateStore) -> None:
        """Set the storage backend (for late binding after Redis init)."""
        self._store = store

    def _storage_key(self, bucket: str, key: str, upload_id: str) -> str:
        """Generate storage key for upload state."""
        return f"{bucket}:{key}:{upload_id}"

    async def list_active_uploads(self) -> list[dict]:
        """List active uploads for admin dashboard. DEKs are never exposed."""
        keys = await self._store.list_keys()
        uploads = []
        for key in keys:
            data = await self._store.get(key)
            if data is None:
                continue
            state = deserialize_upload_state(data)
            if state is None:
                continue
            uploads.append(
                {
                    "bucket": state.bucket,
                    "key": state.key,
                    "upload_id": self._truncate_id(state.upload_id),
                    "parts_count": len(state.parts),
                    "created_at": state.created_at.isoformat(),
                    "total_plaintext_size": state.total_plaintext_size,
                }
            )
        return uploads

    async def create_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        dek: bytes,
    ) -> MultipartUploadState:
        """Create new upload state."""
        state = MultipartUploadState(
            dek=dek,
            bucket=bucket,
            key=key,
            upload_id=upload_id,
        )

        sk = self._storage_key(bucket, key, upload_id)
        await self._store.set(sk, serialize_upload_state(state), self._ttl)

        logger.info(
            "UPLOAD_STATE_CREATED",
            bucket=bucket,
            key=key,
            upload_id=self._truncate_id(upload_id),
        )
        return state

    async def store_reconstructed_state(
        self, bucket: str, key: str, upload_id: str, state: MultipartUploadState
    ) -> None:
        """Store a reconstructed upload state from S3 recovery."""
        sk = self._storage_key(bucket, key, upload_id)
        await self._store.set(sk, serialize_upload_state(state), self._ttl)

        logger.info(
            "UPLOAD_STATE_RECOVERED",
            bucket=bucket,
            key=key,
            upload_id=self._truncate_id(upload_id),
            parts_count=len(state.parts),
        )

    async def get_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> MultipartUploadState | None:
        """Get upload state."""
        sk = self._storage_key(bucket, key, upload_id)
        data = await self._store.get(sk)

        if data is None:
            logger.warning(
                "UPLOAD_STATE_NOT_FOUND",
                bucket=bucket,
                key=key,
                upload_id=self._truncate_id(upload_id),
            )
            return None

        state = deserialize_upload_state(data)
        if state is None:
            logger.error(
                "UPLOAD_STATE_CORRUPTED",
                bucket=bucket,
                key=key,
                upload_id=self._truncate_id(upload_id),
            )
            return None

        logger.debug(
            "UPLOAD_STATE_FOUND",
            bucket=bucket,
            key=key,
            upload_id=self._truncate_id(upload_id),
            parts_count=len(state.parts),
        )
        return state

    async def add_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part: PartMetadata,
    ) -> None:
        """Add part to upload state."""
        sk = self._storage_key(bucket, key, upload_id)
        internal_nums = (
            [ip.internal_part_number for ip in part.internal_parts] if part.internal_parts else []
        )
        max_internal = max(internal_nums) if internal_nums else 0

        logger.debug(
            "ADD_PART_START",
            bucket=bucket,
            key=key,
            upload_id=self._truncate_id(upload_id),
            client_part=part.part_number,
            internal_parts=internal_nums,
        )

        def updater(data: bytes) -> bytes:
            state = deserialize_upload_state(data)
            if state is None:
                raise StateMissingError(f"Upload state corrupted for {bucket}/{key}")

            old_part = state.parts.get(part.part_number)
            if old_part is not None:
                state.total_plaintext_size -= old_part.plaintext_size
            state.parts[part.part_number] = part
            state.total_plaintext_size += part.plaintext_size
            if max_internal >= state.next_internal_part_number:
                state.next_internal_part_number = max_internal + 1

            return serialize_upload_state(state)

        result = await self._store.update(sk, updater, self._ttl)
        if result is None:
            raise StateMissingError(f"Upload state missing for {bucket}/{key}/{upload_id}")

        logger.info(
            "PART_ADDED",
            bucket=bucket,
            key=key,
            upload_id=self._truncate_id(upload_id),
            part_number=part.part_number,
            internal_parts=internal_nums,
        )

    async def complete_upload(
        self, bucket: str, key: str, upload_id: str
    ) -> MultipartUploadState | None:
        """Remove and return upload state on completion."""
        sk = self._storage_key(bucket, key, upload_id)
        data = await self._store.get_and_delete(sk)

        if data is None:
            logger.warning(
                "STATE_NOT_FOUND_ON_COMPLETE",
                bucket=bucket,
                key=key,
                upload_id=self._truncate_id(upload_id),
            )
            return None

        state = deserialize_upload_state(data)
        if state is None:
            return None

        logger.info(
            "UPLOAD_STATE_DELETED",
            bucket=bucket,
            key=key,
            upload_id=self._truncate_id(upload_id),
            parts_count=len(state.parts),
        )
        return state

    async def abort_upload(self, bucket: str, key: str, upload_id: str) -> None:
        """Remove upload state on abort."""
        sk = self._storage_key(bucket, key, upload_id)
        await self._store.delete(sk)

        logger.info(
            "UPLOAD_STATE_ABORTED",
            bucket=bucket,
            key=key,
            upload_id=self._truncate_id(upload_id),
        )

    async def allocate_internal_parts(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        count: int,
        client_part_number: int = 0,
    ) -> int:
        """Allocate internal part numbers based on client part number.

        Each client part gets a reserved range of MAX_INTERNAL_PARTS_PER_CLIENT
        internal part numbers to avoid conflicts.
        """
        if client_part_number > 0:
            start = (client_part_number - 1) * MAX_INTERNAL_PARTS_PER_CLIENT + 1

            if count > MAX_INTERNAL_PARTS_PER_CLIENT:
                logger.warning(
                    "INTERNAL_PARTS_EXCEED_RANGE",
                    bucket=bucket,
                    key=key,
                    client_part=client_part_number,
                    requested=count,
                    max=MAX_INTERNAL_PARTS_PER_CLIENT,
                )

            logger.debug(
                "ALLOCATE_INTERNAL_PARTS",
                bucket=bucket,
                key=key,
                client_part=client_part_number,
                count=count,
                start=start,
                end=start + count - 1,
            )
            return start

        # Fallback: sequential allocation
        return await self._allocate_sequential(bucket, key, upload_id, count)

    async def _allocate_sequential(self, bucket: str, key: str, upload_id: str, count: int) -> int:
        """Allocate internal parts sequentially (fallback when no client part)."""
        sk = self._storage_key(bucket, key, upload_id)
        start = 1

        def updater(data: bytes) -> bytes:
            nonlocal start
            state = deserialize_upload_state(data)
            if state is None:
                return data

            start = state.next_internal_part_number
            state.next_internal_part_number = start + count

            logger.debug(
                "ALLOCATE_SEQUENTIAL",
                bucket=bucket,
                key=key,
                count=count,
                start=start,
                new_next=state.next_internal_part_number,
            )

            return serialize_upload_state(state)

        result = await self._store.update(sk, updater, self._ttl)
        if result is None:
            logger.warning(
                "ALLOCATE_FALLBACK_NO_STATE",
                bucket=bucket,
                key=key,
            )
            return 1

        return start

    @staticmethod
    def _truncate_id(upload_id: str, max_len: int = 20) -> str:
        """Truncate upload ID for logging."""
        return upload_id[:max_len] + "..." if len(upload_id) > max_len else upload_id
