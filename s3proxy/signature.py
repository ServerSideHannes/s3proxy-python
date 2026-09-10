"""Deferred SigV4 verification for large streaming uploads."""

from __future__ import annotations

from urllib.parse import parse_qs

from fastapi import Request

from .client import ParsedRequest, SigV4Verifier
from .errors import S3Error


def deferred_signature_required(request: Request) -> bool:
    return getattr(request.state, "s3proxy_deferred_sig", False) is True


def verify_deferred_payload_hash(
    request: Request, verifier: SigV4Verifier, payload_hash: str
) -> None:
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


def verify_payload_hash(request: Request, payload_hash: str) -> None:
    """Verify the body before publishing any new object or part state."""
    import hmac

    if deferred_signature_required(request):
        verify_deferred_payload_hash(request, request.app.state.verifier, payload_hash)
        return
    expected = request.headers.get("x-amz-content-sha256", "")
    if (
        expected
        and expected != "UNSIGNED-PAYLOAD"
        and not expected.startswith("STREAMING-")
        and not hmac.compare_digest(expected, payload_hash)
    ):
        raise S3Error.signature_does_not_match("Payload SHA256 mismatch")
