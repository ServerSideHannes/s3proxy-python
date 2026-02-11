"""ListParts handler for multipart uploads."""

from urllib.parse import parse_qs

import structlog
from botocore.exceptions import ClientError
from fastapi import Request, Response
from structlog.stdlib import BoundLogger

from ... import xml_responses
from ...errors import S3Error
from ...s3client import S3Credentials
from ..base import BaseHandler

logger: BoundLogger = structlog.get_logger(__name__)


class ListPartsMixin(BaseHandler):
    async def handle_list_parts(self, request: Request, creds: S3Credentials) -> Response:
        bucket, key = self._parse_path(request.url.path)
        async with self._client(creds) as client:
            query = parse_qs(request.url.query)
            upload_id = query.get("uploadId", [""])[0]
            part_number_marker = query.get("part-number-marker", [""])[0]
            part_number_marker = int(part_number_marker) if part_number_marker else None
            max_parts = int(query.get("max-parts", ["1000"])[0])

            try:
                resp = await client.list_parts(
                    bucket, key, upload_id, part_number_marker, max_parts
                )
            except ClientError as e:
                if e.response["Error"]["Code"] in ("NoSuchUpload", "404"):
                    raise S3Error.no_such_upload(upload_id) from None
                raise S3Error.internal_error(str(e)) from e

            parts = []
            for part in resp.get("Parts", []):
                last_modified = part.get("LastModified")
                if hasattr(last_modified, "isoformat"):
                    last_modified = last_modified.isoformat().replace("+00:00", "Z")
                else:
                    last_modified = str(last_modified) if last_modified else ""

                parts.append(
                    {
                        "PartNumber": part.get("PartNumber", 0),
                        "LastModified": last_modified,
                        "ETag": part.get("ETag", "").strip('"'),
                        "Size": part.get("Size", 0),
                    }
                )

            return Response(
                content=xml_responses.list_parts(
                    bucket=bucket,
                    key=key,
                    upload_id=upload_id,
                    parts=parts,
                    part_number_marker=part_number_marker,
                    next_part_number_marker=resp.get("NextPartNumberMarker"),
                    max_parts=max_parts,
                    is_truncated=resp.get("IsTruncated", False),
                    storage_class=resp.get("StorageClass", "STANDARD"),
                ),
                media_type="application/xml",
            )
