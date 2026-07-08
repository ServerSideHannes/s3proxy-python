"""Deferred SigV4 verification for large streaming uploads."""

from __future__ import annotations

from urllib.parse import parse_qs

from fastapi import Request

from .client import ParsedRequest, SigV4Verifier
from .errors import S3Error


def deferred_signature_required(request: Request) -> bool:
    return bool(getattr(request.state, "s3proxy_deferred_sig", False))


def verify_deferred_payload_hash(request: Request, verifier: SigV4Verifier, payload_hash: str) -> None:
    """Verify a request whose body was streamed after computing its SHA256."""
    if not deferred_signature_required(request):
        return

    headers = {k.lower(): v for k, v in request.headers.items()}
    query = parse_qs(str(request.url.query), keep_blank_values=True)
    parsed = ParsedRequest(
        method=request.method,
        bucket="",
        key="",
        query_params=query,
        headers=headers,
        body=b"",
    )
    sig_path = getattr(request.state, "s3proxy_sig_path", request.url.path)
    auth_header = headers.get("authorization", "")
    valid, _, error = verifier.verify_with_payload_hash(parsed, sig_path, auth_header, payload_hash)
    if not valid:
        if error and "signature" in error.lower():
            raise S3Error.signature_does_not_match(error)
        raise S3Error.access_denied(error or "Access Denied")
