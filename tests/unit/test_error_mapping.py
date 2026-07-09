"""S3 error mapping and internal part allocation validation."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from s3proxy import crypto
from s3proxy.errors import S3Error, raise_for_client_error


def _client_error(code: str, message: str = "msg") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}, "ResponseMetadata": {"HTTPStatusCode": 400}},
        "UploadPart",
    )


@pytest.mark.parametrize(
    ("code", "status", "s3_code"),
    [
        ("InvalidPart", 400, "InvalidPart"),
        ("InvalidPartNumber", 400, "InvalidPart"),
        ("EntityTooSmall", 400, "EntityTooSmall"),
        ("NoSuchUpload", 404, "NoSuchUpload"),
    ],
)
def test_raise_for_client_error_maps_known_codes(code, status, s3_code):
    with pytest.raises(S3Error) as exc:
        raise_for_client_error(_client_error(code), bucket="b", key="k")
    assert exc.value.status_code == status
    assert exc.value.code == s3_code


def test_validate_internal_part_allocation_rejects_over_s3_limit():
    # Client part 501 needs internal parts starting at 10001.
    with pytest.raises(ValueError, match="10001"):
        crypto.validate_internal_part_allocation(501, 1)


def test_validate_internal_part_allocation_allows_part_418():
    start, end = crypto.validate_internal_part_allocation(418, 20)
    assert start == 8341
    assert end == 8360
