"""Deferred SigV4 verification after streaming body hash."""

import hashlib
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from s3proxy.client import ParsedRequest, SigV4Verifier
from s3proxy.signature import verify_deferred_payload_hash


@pytest.mark.asyncio
async def test_verify_deferred_payload_hash_accepts_valid_signature():
    verifier = SigV4Verifier({"testkey": "testsecret"})
    payload = b"scylla-sst-bytes"
    payload_hash = hashlib.sha256(payload).hexdigest()
    amz_date = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]
    parsed = ParsedRequest(
        method="PUT",
        bucket="",
        key="",
        headers={
            "host": "s3proxy.test",
            "x-amz-date": amz_date,
            "content-length": str(len(payload)),
        },
        body=payload,
    )
    path = "/bucket/key"
    canonical = verifier._build_canonical_request(parsed, path, ["host", "x-amz-date"])
    signature = verifier._compute_v4_signature(
        canonical,
        amz_date,
        date_stamp,
        "us-east-1",
        "s3",
        "testsecret",
    )
    auth_header = (
        "AWS4-HMAC-SHA256 "
        f"Credential=testkey/{date_stamp}/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;x-amz-date, "
        f"Signature={signature}"
    )

    request = MagicMock()
    request.method = "PUT"
    request.url = MagicMock()
    request.url.path = path
    request.url.query = ""
    request.headers = {
        "authorization": auth_header,
        "x-amz-date": amz_date,
        "host": "s3proxy.test",
        "content-length": str(len(payload)),
    }
    request.state = MagicMock()
    request.state.s3proxy_deferred_sig = True
    request.state.s3proxy_sig_path = path

    verify_deferred_payload_hash(request, verifier, payload_hash)
