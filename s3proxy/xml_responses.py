"""S3 XML response builders."""

from urllib.parse import quote
from xml.sax.saxutils import escape

# Common XML constants
_XML_HEADER = '<?xml version="1.0" encoding="UTF-8"?>'
_S3_NS = 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"'


def _encode_key(key: str, encoding_type: str | None) -> str:
    """URL-encode key if encoding_type is 'url'."""
    if encoding_type == "url":
        return quote(key, safe="")
    return escape(key)


def initiate_multipart(bucket: str, key: str, upload_id: str) -> str:
    """Build InitiateMultipartUploadResult XML."""
    return f"""{_XML_HEADER}
<InitiateMultipartUploadResult {_S3_NS}>
    <Bucket>{escape(bucket)}</Bucket>
    <Key>{escape(key)}</Key>
    <UploadId>{escape(upload_id)}</UploadId>
</InitiateMultipartUploadResult>"""


def complete_multipart(location: str, bucket: str, key: str, etag: str) -> str:
    """Build CompleteMultipartUploadResult XML."""
    return f"""{_XML_HEADER}
<CompleteMultipartUploadResult {_S3_NS}>
    <Location>{escape(location)}</Location>
    <Bucket>{escape(bucket)}</Bucket>
    <Key>{escape(key)}</Key>
    <ETag>"{escape(etag)}"</ETag>
</CompleteMultipartUploadResult>"""


def list_objects(
    bucket: str,
    prefix: str,
    max_keys: int,
    is_truncated: bool,
    next_token: str | None,
    objects: list[dict],
    delimiter: str | None = None,
    common_prefixes: list[str] | None = None,
    continuation_token: str | None = None,
    start_after: str | None = None,
    encoding_type: str | None = None,
    fetch_owner: bool = False,
) -> str:
    """Build ListBucketResult XML for V2 API."""
    objects_xml = ""
    for obj in objects:
        key_encoded = _encode_key(obj["key"], encoding_type)
        owner_xml = ""
        if fetch_owner:
            owner_xml = """
        <Owner>
            <ID>owner-id</ID>
            <DisplayName>owner-name</DisplayName>
        </Owner>"""
        objects_xml += f"""
    <Contents>
        <Key>{key_encoded}</Key>
        <LastModified>{obj["last_modified"]}</LastModified>
        <ETag>"{obj["etag"]}"</ETag>
        <Size>{obj["size"]}</Size>
        <StorageClass>{obj.get("storage_class", "STANDARD")}</StorageClass>{owner_xml}
    </Contents>"""

    next_token_xml = (
        f"<NextContinuationToken>{_encode_key(next_token, encoding_type)}</NextContinuationToken>"
        if next_token
        else ""
    )

    continuation_token_xml = (
        f"<ContinuationToken>{_encode_key(continuation_token, encoding_type)}</ContinuationToken>"
        if continuation_token is not None
        else ""
    )

    start_after_xml = (
        f"<StartAfter>{_encode_key(start_after, encoding_type)}</StartAfter>" if start_after else ""
    )

    # Note: Delimiter is NOT URL-encoded even with encoding-type=url per S3 spec
    delimiter_xml = f"<Delimiter>{escape(delimiter)}</Delimiter>" if delimiter else ""
    encoding_xml = f"<EncodingType>{encoding_type}</EncodingType>" if encoding_type else ""

    prefixes_xml = ""
    if common_prefixes:
        for cp in common_prefixes:
            prefixes_xml += f"""
    <CommonPrefixes>
        <Prefix>{_encode_key(cp, encoding_type)}</Prefix>
    </CommonPrefixes>"""

    # Note: Prefix is echoed back as-is, not URL-encoded (per S3 behavior)
    return f"""{_XML_HEADER}
<ListBucketResult {_S3_NS}>
    <Name>{escape(bucket)}</Name>
    <Prefix>{escape(prefix)}</Prefix>
    {delimiter_xml}
    {start_after_xml}
    {encoding_xml}
    <MaxKeys>{max_keys}</MaxKeys>
    {continuation_token_xml}
    <IsTruncated>{str(is_truncated).lower()}</IsTruncated>
    {next_token_xml}
    <KeyCount>{len(objects) + len(common_prefixes or [])}</KeyCount>{objects_xml}{prefixes_xml}
</ListBucketResult>"""


def location_constraint(location: str | None) -> str:
    """Build LocationConstraint XML for GetBucketLocation."""
    # AWS returns empty LocationConstraint for us-east-1
    if location is None or location == "us-east-1" or location == "":
        return f"{_XML_HEADER}\n<LocationConstraint {_S3_NS}/>"
    return f"{_XML_HEADER}\n<LocationConstraint {_S3_NS}>{escape(location)}</LocationConstraint>"


def copy_result(etag: str, last_modified: str, is_part: bool = False) -> str:
    """Build CopyObjectResult or CopyPartResult XML."""
    tag = "CopyPartResult" if is_part else "CopyObjectResult"
    return f"""{_XML_HEADER}
<{tag} {_S3_NS}>
    <ETag>"{etag}"</ETag>
    <LastModified>{last_modified}</LastModified>
</{tag}>"""


# Backwards compatibility aliases
def copy_object_result(etag: str, last_modified: str) -> str:
    return copy_result(etag, last_modified, is_part=False)


def delete_objects_result(
    deleted: list[dict[str, str]],
    errors: list[dict[str, str]],
    quiet: bool = False,
) -> str:
    """Build DeleteResult XML for batch delete."""
    deleted_xml = ""
    if not quiet:
        for obj in deleted:
            deleted_xml += f"""
    <Deleted>
        <Key>{escape(obj["Key"])}</Key>"""
            if obj.get("VersionId"):
                deleted_xml += f"""
        <VersionId>{obj["VersionId"]}</VersionId>"""
            deleted_xml += """
    </Deleted>"""

    errors_xml = ""
    for err in errors:
        errors_xml += f"""
    <Error>
        <Key>{escape(err["Key"])}</Key>
        <Code>{err.get("Code", "InternalError")}</Code>
        <Message>{escape(err.get("Message", ""))}</Message>"""
        if err.get("VersionId"):
            errors_xml += f"""
        <VersionId>{err["VersionId"]}</VersionId>"""
        errors_xml += """
    </Error>"""

    return f"""{_XML_HEADER}
<DeleteResult {_S3_NS}>{deleted_xml}{errors_xml}
</DeleteResult>"""


def list_multipart_uploads(
    bucket: str,
    uploads: list[dict],
    key_marker: str | None,
    upload_id_marker: str | None,
    next_key_marker: str | None,
    next_upload_id_marker: str | None,
    max_uploads: int,
    is_truncated: bool,
    prefix: str | None = None,
) -> str:
    """Build ListMultipartUploadsResult XML."""
    uploads_xml = ""
    for upload in uploads:
        uploads_xml += f"""
    <Upload>
        <Key>{escape(upload["Key"])}</Key>
        <UploadId>{upload["UploadId"]}</UploadId>
        <Initiated>{upload["Initiated"]}</Initiated>"""
        if upload.get("StorageClass"):
            uploads_xml += f"""
        <StorageClass>{upload["StorageClass"]}</StorageClass>"""
        uploads_xml += """
    </Upload>"""

    prefix_xml = f"<Prefix>{escape(prefix or '')}</Prefix>"
    key_marker_xml = f"<KeyMarker>{escape(key_marker or '')}</KeyMarker>"
    upload_id_marker_xml = f"<UploadIdMarker>{escape(upload_id_marker or '')}</UploadIdMarker>"

    next_markers_xml = ""
    if is_truncated and next_key_marker:
        next_markers_xml += f"<NextKeyMarker>{escape(next_key_marker)}</NextKeyMarker>"
    if is_truncated and next_upload_id_marker:
        next_markers_xml += (
            f"<NextUploadIdMarker>{escape(next_upload_id_marker)}</NextUploadIdMarker>"
        )

    return f"""{_XML_HEADER}
<ListMultipartUploadsResult {_S3_NS}>
    <Bucket>{escape(bucket)}</Bucket>
    {key_marker_xml}
    {upload_id_marker_xml}
    {next_markers_xml}
    <MaxUploads>{max_uploads}</MaxUploads>
    <IsTruncated>{str(is_truncated).lower()}</IsTruncated>
    {prefix_xml}{uploads_xml}
</ListMultipartUploadsResult>"""


def list_parts(
    bucket: str,
    key: str,
    upload_id: str,
    parts: list[dict],
    part_number_marker: int | None,
    next_part_number_marker: int | None,
    max_parts: int,
    is_truncated: bool,
    storage_class: str = "STANDARD",
) -> str:
    """Build ListPartsResult XML."""
    parts_xml = ""
    for part in parts:
        parts_xml += f"""
    <Part>
        <PartNumber>{part["PartNumber"]}</PartNumber>
        <LastModified>{part["LastModified"]}</LastModified>
        <ETag>"{part["ETag"]}"</ETag>
        <Size>{part["Size"]}</Size>
    </Part>"""

    marker_xml = ""
    if part_number_marker:
        marker_xml = f"<PartNumberMarker>{part_number_marker}</PartNumberMarker>"

    next_marker_xml = ""
    if is_truncated and next_part_number_marker:
        next_marker_xml = f"<NextPartNumberMarker>{next_part_number_marker}</NextPartNumberMarker>"

    return f"""{_XML_HEADER}
<ListPartsResult {_S3_NS}>
    <Bucket>{escape(bucket)}</Bucket>
    <Key>{escape(key)}</Key>
    <UploadId>{escape(upload_id)}</UploadId>
    {marker_xml}
    {next_marker_xml}
    <MaxParts>{max_parts}</MaxParts>
    <IsTruncated>{str(is_truncated).lower()}</IsTruncated>
    <StorageClass>{storage_class}</StorageClass>{parts_xml}
</ListPartsResult>"""


def list_buckets(owner: dict, buckets: list[dict]) -> str:
    """Build ListAllMyBucketsResult XML."""
    buckets_xml = ""
    for b in buckets:
        creation_date = b.get("CreationDate", "")
        if hasattr(creation_date, "isoformat"):
            creation_date = creation_date.isoformat()
        buckets_xml += f"""
        <Bucket>
            <Name>{escape(b.get("Name", ""))}</Name>
            <CreationDate>{creation_date}</CreationDate>
        </Bucket>"""

    return f"""{_XML_HEADER}
<ListAllMyBucketsResult {_S3_NS}>
    <Owner>
        <ID>{escape(owner.get("ID", ""))}</ID>
        <DisplayName>{escape(owner.get("DisplayName", ""))}</DisplayName>
    </Owner>
    <Buckets>{buckets_xml}
    </Buckets>
</ListAllMyBucketsResult>"""


def list_objects_v1(
    bucket: str,
    prefix: str,
    marker: str | None,
    delimiter: str | None,
    max_keys: int,
    is_truncated: bool,
    next_marker: str | None,
    objects: list[dict],
    common_prefixes: list[str] | None = None,
    encoding_type: str | None = None,
) -> str:
    """Build ListBucketResult XML for V1 API."""
    objects_xml = ""
    for obj in objects:
        key_encoded = _encode_key(obj["key"], encoding_type)
        objects_xml += f"""
    <Contents>
        <Key>{key_encoded}</Key>
        <LastModified>{obj["last_modified"]}</LastModified>
        <ETag>"{obj["etag"]}"</ETag>
        <Size>{obj["size"]}</Size>
        <StorageClass>{obj.get("storage_class", "STANDARD")}</StorageClass>
    </Contents>"""

    # Note: Marker is echoed back as-is, not URL-encoded (per S3 behavior)
    marker_xml = f"<Marker>{escape(marker or '')}</Marker>"
    next_marker_xml = (
        f"<NextMarker>{_encode_key(next_marker or '', encoding_type)}</NextMarker>"
        if next_marker
        else ""
    )
    # Note: Delimiter is NOT URL-encoded even with encoding-type=url per S3 spec
    delimiter_xml = f"<Delimiter>{escape(delimiter)}</Delimiter>" if delimiter else ""
    encoding_xml = f"<EncodingType>{encoding_type}</EncodingType>" if encoding_type else ""

    prefixes_xml = ""
    if common_prefixes:
        for cp in common_prefixes:
            prefixes_xml += f"""
    <CommonPrefixes>
        <Prefix>{_encode_key(cp, encoding_type)}</Prefix>
    </CommonPrefixes>"""

    # Note: Prefix is echoed back as-is, not URL-encoded (per S3 behavior)
    return f"""{_XML_HEADER}
<ListBucketResult {_S3_NS}>
    <Name>{escape(bucket)}</Name>
    <Prefix>{escape(prefix)}</Prefix>
    {marker_xml}
    {delimiter_xml}
    {encoding_xml}
    <MaxKeys>{max_keys}</MaxKeys>
    <IsTruncated>{str(is_truncated).lower()}</IsTruncated>
    {next_marker_xml}{objects_xml}{prefixes_xml}
</ListBucketResult>"""


def get_tagging(tags: list[dict]) -> str:
    """Build GetObjectTaggingResult XML."""
    tags_xml = ""
    for tag in tags:
        tags_xml += f"""
        <Tag>
            <Key>{escape(tag.get("Key", ""))}</Key>
            <Value>{escape(tag.get("Value", ""))}</Value>
        </Tag>"""

    return f"""{_XML_HEADER}
<Tagging {_S3_NS}>
    <TagSet>{tags_xml}
    </TagSet>
</Tagging>"""


def upload_part_copy_result(etag: str, last_modified: str) -> str:
    return copy_result(etag, last_modified, is_part=True)
