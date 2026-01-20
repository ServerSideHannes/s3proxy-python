"""S3 XML response builders."""

from xml.sax.saxutils import escape


def initiate_multipart(bucket: str, key: str, upload_id: str) -> str:
    """Build InitiateMultipartUploadResult XML."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{bucket}</Bucket>
    <Key>{key}</Key>
    <UploadId>{upload_id}</UploadId>
</InitiateMultipartUploadResult>"""


def complete_multipart(location: str, bucket: str, key: str, etag: str) -> str:
    """Build CompleteMultipartUploadResult XML."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Location>{location}</Location>
    <Bucket>{bucket}</Bucket>
    <Key>{key}</Key>
    <ETag>"{etag}"</ETag>
</CompleteMultipartUploadResult>"""


def list_objects(
    bucket: str,
    prefix: str,
    max_keys: int,
    is_truncated: bool,
    next_token: str | None,
    objects: list[dict],
) -> str:
    """Build ListBucketResult XML."""
    objects_xml = ""
    for obj in objects:
        objects_xml += f"""
    <Contents>
        <Key>{obj["key"]}</Key>
        <LastModified>{obj["last_modified"]}</LastModified>
        <ETag>"{obj["etag"]}"</ETag>
        <Size>{obj["size"]}</Size>
        <StorageClass>{obj.get("storage_class", "STANDARD")}</StorageClass>
    </Contents>"""

    next_token_xml = (
        f"<NextContinuationToken>{next_token}</NextContinuationToken>"
        if next_token else ""
    )

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>{bucket}</Name>
    <Prefix>{prefix}</Prefix>
    <MaxKeys>{max_keys}</MaxKeys>
    <IsTruncated>{str(is_truncated).lower()}</IsTruncated>
    {next_token_xml}
    <KeyCount>{len(objects)}</KeyCount>{objects_xml}
</ListBucketResult>"""


def location_constraint(location: str | None) -> str:
    """Build LocationConstraint XML for GetBucketLocation."""
    # AWS returns empty LocationConstraint for us-east-1
    if location is None or location == "us-east-1" or location == "":
        return """<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{location}</LocationConstraint>"""


def copy_object_result(etag: str, last_modified: str) -> str:
    """Build CopyObjectResult XML."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <ETag>"{etag}"</ETag>
    <LastModified>{last_modified}</LastModified>
</CopyObjectResult>"""


def delete_objects_result(
    deleted: list[dict[str, str]],
    errors: list[dict[str, str]],
    quiet: bool = False,
) -> str:
    """Build DeleteResult XML for batch delete.

    Args:
        deleted: List of {"Key": key, "VersionId": vid} for deleted objects
        errors: List of {"Key": key, "Code": code, "Message": msg} for failures
        quiet: If True, don't include deleted objects in response
    """
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

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{deleted_xml}{errors_xml}
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
    """Build ListMultipartUploadsResult XML.

    Args:
        bucket: Bucket name
        uploads: List of upload dicts with Key, UploadId, Initiated, etc.
        key_marker: KeyMarker from request
        upload_id_marker: UploadIdMarker from request
        next_key_marker: NextKeyMarker for pagination
        next_upload_id_marker: NextUploadIdMarker for pagination
        max_uploads: MaxUploads from request
        is_truncated: Whether there are more results
        prefix: Optional prefix filter
    """
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
        next_markers_xml += f"<NextUploadIdMarker>{escape(next_upload_id_marker)}</NextUploadIdMarker>"

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{bucket}</Bucket>
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
    """Build ListPartsResult XML.

    Args:
        bucket: Bucket name
        key: Object key
        upload_id: Multipart upload ID
        parts: List of part dicts with PartNumber, ETag, Size, LastModified
        part_number_marker: PartNumberMarker from request
        next_part_number_marker: NextPartNumberMarker for pagination
        max_parts: MaxParts from request
        is_truncated: Whether there are more results
        storage_class: Storage class
    """
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

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Bucket>{bucket}</Bucket>
    <Key>{escape(key)}</Key>
    <UploadId>{upload_id}</UploadId>
    {marker_xml}
    {next_marker_xml}
    <MaxParts>{max_parts}</MaxParts>
    <IsTruncated>{str(is_truncated).lower()}</IsTruncated>
    <StorageClass>{storage_class}</StorageClass>{parts_xml}
</ListPartsResult>"""


def list_buckets(owner: dict, buckets: list[dict]) -> str:
    """Build ListAllMyBucketsResult XML.

    Args:
        owner: Owner dict with ID and DisplayName
        buckets: List of bucket dicts with Name and CreationDate
    """
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

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
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
) -> str:
    """Build ListBucketResult XML for V1 API.

    Args:
        bucket: Bucket name
        prefix: Prefix filter
        marker: Marker from request
        delimiter: Delimiter for grouping
        max_keys: Max keys requested
        is_truncated: Whether there are more results
        next_marker: Next marker for pagination
        objects: List of object dicts
        common_prefixes: List of common prefix strings
    """
    objects_xml = ""
    for obj in objects:
        objects_xml += f"""
    <Contents>
        <Key>{escape(obj["key"])}</Key>
        <LastModified>{obj["last_modified"]}</LastModified>
        <ETag>"{obj["etag"]}"</ETag>
        <Size>{obj["size"]}</Size>
        <StorageClass>{obj.get("storage_class", "STANDARD")}</StorageClass>
    </Contents>"""

    marker_xml = f"<Marker>{escape(marker or '')}</Marker>"
    next_marker_xml = f"<NextMarker>{escape(next_marker or '')}</NextMarker>" if next_marker else ""
    delimiter_xml = f"<Delimiter>{escape(delimiter)}</Delimiter>" if delimiter else ""

    prefixes_xml = ""
    if common_prefixes:
        for cp in common_prefixes:
            prefixes_xml += f"""
    <CommonPrefixes>
        <Prefix>{escape(cp)}</Prefix>
    </CommonPrefixes>"""

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>{bucket}</Name>
    <Prefix>{escape(prefix)}</Prefix>
    {marker_xml}
    {delimiter_xml}
    <MaxKeys>{max_keys}</MaxKeys>
    <IsTruncated>{str(is_truncated).lower()}</IsTruncated>
    {next_marker_xml}{objects_xml}{prefixes_xml}
</ListBucketResult>"""


def get_tagging(tags: list[dict]) -> str:
    """Build GetObjectTaggingResult XML.

    Args:
        tags: List of tag dicts with Key and Value
    """
    tags_xml = ""
    for tag in tags:
        tags_xml += f"""
        <Tag>
            <Key>{escape(tag.get("Key", ""))}</Key>
            <Value>{escape(tag.get("Value", ""))}</Value>
        </Tag>"""

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <TagSet>{tags_xml}
    </TagSet>
</Tagging>"""


def upload_part_copy_result(etag: str, last_modified: str) -> str:
    """Build CopyPartResult XML."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <ETag>"{etag}"</ETag>
    <LastModified>{last_modified}</LastModified>
</CopyPartResult>"""
