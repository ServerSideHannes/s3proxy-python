"""UploadPartCopy handler for multipart uploads."""

import hashlib
from datetime import UTC, datetime

import structlog
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import crypto, xml_responses
from ...errors import S3Error
from ...s3client import S3Credentials
from ...state import (
    PartMetadata,
    load_multipart_metadata,
    load_upload_state,
)
from ...utils import format_iso8601
from ..base import BaseHandler

logger: BoundLogger = structlog.get_logger(__name__)


class CopyPartMixin(BaseHandler):
    async def handle_upload_part_copy(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            upload_id, part_num = self._extract_multipart_params(request)

            copy_source = request.headers.get("x-amz-copy-source", "")
            copy_source_range = request.headers.get("x-amz-copy-source-range")

            src_bucket, src_key = self._parse_copy_source(copy_source)

            # Get upload state
            state = await self.multipart_manager.get_upload(bucket, key, upload_id)
            if not state:
                dek = await load_upload_state(client, bucket, key, upload_id, self.settings.kek)
                if not dek:
                    raise S3Error.no_such_upload(upload_id)
                state = await self.multipart_manager.create_upload(bucket, key, upload_id, dek)

            # Get source data
            plaintext = await self._get_copy_source_data(
                client, src_bucket, src_key, copy_source_range
            )

            # Encrypt and upload
            ciphertext = crypto.encrypt_part(plaintext, state.dek, upload_id, part_num)
            resp = await client.upload_part(bucket, key, upload_id, part_num, ciphertext)

            body_md5 = hashlib.md5(plaintext, usedforsecurity=False).hexdigest()
            await self.multipart_manager.add_part(
                bucket,
                key,
                upload_id,
                PartMetadata(
                    part_num, len(plaintext), len(ciphertext), resp["ETag"].strip('"'), body_md5
                ),
            )

            last_modified = format_iso8601(datetime.now(UTC))
            return Response(
                content=xml_responses.upload_part_copy_result(
                    resp["ETag"].strip('"'), last_modified
                ),
                media_type="application/xml",
            )

    async def _get_copy_source_data(
        self, client, src_bucket: str, src_key: str, copy_source_range: str | None
    ) -> bytes:
        try:
            head_resp = await client.head_object(src_bucket, src_key)
        except Exception as e:
            raise S3Error.no_such_key(src_key) from e

        src_metadata = head_resp.get("Metadata", {})
        src_wrapped_dek = src_metadata.get(self.settings.dektag_name)
        src_multipart_meta = await load_multipart_metadata(client, src_bucket, src_key)

        if not src_wrapped_dek and not src_multipart_meta:
            # Not encrypted
            resp = await client.get_object(src_bucket, src_key, range_header=copy_source_range)
            async with resp["Body"] as body:
                return await body.read()
        elif src_multipart_meta:
            # Multipart encrypted - use shared helper with range support
            range_start, range_end = self._parse_copy_source_range(
                copy_source_range, src_multipart_meta.total_plaintext_size
            )
            return await self._download_encrypted_multipart(
                client, src_bucket, src_key, src_multipart_meta, range_start, range_end
            )
        else:
            # Single-part encrypted - use shared helper
            full_plaintext = await self._download_encrypted_single(
                client, src_bucket, src_key, src_wrapped_dek
            )
            if copy_source_range:
                range_start, range_end = self._parse_copy_source_range(
                    copy_source_range, len(full_plaintext)
                )
                return full_plaintext[range_start : range_end + 1]
            return full_plaintext
