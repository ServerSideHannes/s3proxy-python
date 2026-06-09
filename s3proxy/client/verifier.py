"""AWS Signature Version 4 verification."""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from urllib.parse import quote, unquote

import structlog
from structlog.stdlib import BoundLogger

from .types import ParsedRequest, S3Credentials

logger: BoundLogger = structlog.get_logger(__name__)

# SigV4 clock skew tolerance
CLOCK_SKEW_TOLERANCE = timedelta(minutes=5)


@lru_cache(maxsize=64)
def _derive_signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    """Derive SigV4 signing key with caching.

    The signing key only depends on (secret_key, date_stamp, region, service) and
    stays the same for an entire day. Caching avoids 4 HMAC operations per request.
    """
    k_date = hmac.new(f"AWS4{secret_key}".encode(), date_stamp.encode(), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
    return hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()


class SigV4Verifier:
    """AWS Signature Version 4 verification."""

    def __init__(self, credentials_store: dict[str, str]):
        """Initialize with a mapping of access_key -> secret_key."""
        self.credentials_store = credentials_store

    def _parse_v4_credential(
        self, credential: str
    ) -> tuple[S3Credentials | None, str, str, str, str | None]:
        """Parse V4 credential string and lookup secret key.

        Returns: (credentials, date_stamp, region, service, error)
        """
        try:
            parts = credential.split("/")
            access_key, date_stamp, region, service = parts[0], parts[1], parts[2], parts[3]
        except IndexError, ValueError:
            return None, "", "", "", "Invalid credential format"

        secret_key = self.credentials_store.get(access_key)
        if not secret_key:
            return None, "", "", "", f"Unknown access key: {access_key}"

        creds = S3Credentials(
            access_key=access_key, secret_key=secret_key, region=region, service=service
        )
        return creds, date_stamp, region, service, None

    def _compute_v4_signature(
        self,
        canonical_request: str,
        amz_date: str,
        date_stamp: str,
        region: str,
        service: str,
        secret_key: str,
    ) -> str:
        """Compute V4 signature for a canonical request."""
        string_to_sign = self._build_string_to_sign(
            amz_date, date_stamp, region, service, canonical_request
        )
        signing_key = _derive_signing_key(secret_key, date_stamp, region, service)
        return hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()

    def verify(self, request: ParsedRequest, path: str) -> tuple[bool, S3Credentials | None, str]:
        """Verify SigV4 signature. Returns (is_valid, credentials, error_message)."""
        # Check for Authorization header (standard SigV4)
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("AWS4-HMAC-SHA256"):
            return self._verify_header_signature(request, path, auth_header)

        # Check for presigned URL (query params)
        if "X-Amz-Signature" in request.query_params:
            return self._verify_presigned_v4(request, path)

        # Check for legacy presigned V2
        if "Signature" in request.query_params:
            return self._verify_presigned_v2(request, path)

        return False, None, "No AWS signature found"

    def _verify_header_signature(
        self, request: ParsedRequest, path: str, auth_header: str
    ) -> tuple[bool, S3Credentials | None, str]:
        """Verify Authorization header signature."""
        try:
            parts = auth_header.replace("AWS4-HMAC-SHA256 ", "").split(",")
            auth_parts = {}
            for part in parts:
                key, value = part.strip().split("=", 1)
                auth_parts[key.strip()] = value.strip()

            credential = auth_parts["Credential"]
            signed_headers = auth_parts["SignedHeaders"]
            signature = auth_parts["Signature"]

            credentials, date_stamp, region, service, error = self._parse_v4_credential(credential)
            if error:
                return False, None, error

            amz_date = request.headers.get("x-amz-date", "")
            if not amz_date:
                return False, credentials, "Missing x-amz-date header"

            try:
                request_time = datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
                if abs(datetime.now(UTC) - request_time) > CLOCK_SKEW_TOLERANCE:
                    return False, credentials, "Request time too skewed"
            except ValueError:
                return False, credentials, "Invalid x-amz-date format"

            canonical_request = self._build_canonical_request(
                request, path, signed_headers.split(";")
            )
            calculated_sig = self._compute_v4_signature(
                canonical_request, amz_date, date_stamp, region, service, credentials.secret_key
            )

            if hmac.compare_digest(calculated_sig, signature):
                return True, credentials, ""

            logger.debug(
                "Signature verification failed",
                method=request.method,
                path=path,
                signed_headers=signed_headers,
                expected_sig=signature[:16] + "...",
                calculated_sig=calculated_sig[:16] + "...",
            )
            return False, credentials, "Signature mismatch"

        except (KeyError, ValueError, IndexError) as e:
            return False, None, f"Invalid Authorization header: {e}"

    def _verify_presigned_v4(
        self, request: ParsedRequest, path: str
    ) -> tuple[bool, S3Credentials | None, str]:
        """Verify presigned URL (V4)."""
        try:
            credential = request.query_params.get("X-Amz-Credential", [""])[0]
            amz_date = request.query_params.get("X-Amz-Date", [""])[0]
            expires = int(request.query_params.get("X-Amz-Expires", ["0"])[0])
            signed_headers = request.query_params.get("X-Amz-SignedHeaders", [""])[0]
            signature = request.query_params.get("X-Amz-Signature", [""])[0]

            credentials, date_stamp, region, service, error = self._parse_v4_credential(credential)
            if error:
                return False, None, error

            request_time = datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
            if datetime.now(UTC) > request_time + timedelta(seconds=expires):
                return False, credentials, "Presigned URL expired"

            query_for_signing = {
                k: v for k, v in request.query_params.items() if k != "X-Amz-Signature"
            }
            signed_headers_list = signed_headers.split(";")

            # Try verification with original headers first
            canonical_request = self._build_canonical_request_presigned(
                request, path, signed_headers_list, query_for_signing
            )
            calculated_sig = self._compute_v4_signature(
                canonical_request, amz_date, date_stamp, region, service, credentials.secret_key
            )

            if hmac.compare_digest(calculated_sig, signature):
                return True, credentials, ""

            # Try with alternate host header (with/without :80) for HTTP port normalization
            host_header = request.headers.get("host", "")
            if "host" in signed_headers_list:
                alternate_host = (
                    host_header[:-3]
                    if host_header.endswith(":80")
                    else host_header + ":80"
                    if ":" not in host_header
                    else None
                )
                if alternate_host:
                    modified_headers = dict(request.headers)
                    modified_headers["host"] = alternate_host
                    modified_request = ParsedRequest(
                        method=request.method,
                        bucket=request.bucket,
                        key=request.key,
                        query_params=request.query_params,
                        headers=modified_headers,
                        body=request.body,
                        is_presigned=request.is_presigned,
                    )
                    canonical_request_alt = self._build_canonical_request_presigned(
                        modified_request, path, signed_headers_list, query_for_signing
                    )
                    calculated_sig_alt = self._compute_v4_signature(
                        canonical_request_alt,
                        amz_date,
                        date_stamp,
                        region,
                        service,
                        credentials.secret_key,
                    )
                    if hmac.compare_digest(calculated_sig_alt, signature):
                        return True, credentials, ""

            logger.warning(
                "Presigned URL signature mismatch",
                path=path,
                signed_headers=signed_headers,
                host_header=host_header,
                expected_sig=signature[:16] + "...",
                calculated_sig=calculated_sig[:16] + "...",
            )
            return False, credentials, "Signature mismatch"

        except (KeyError, ValueError, IndexError) as e:
            return False, None, f"Invalid presigned URL: {e}"

    def _verify_presigned_v2(
        self, request: ParsedRequest, path: str
    ) -> tuple[bool, S3Credentials | None, str]:
        """Verify legacy presigned URL (V2)."""
        try:
            access_key = request.query_params.get("AWSAccessKeyId", [""])[0]
            signature = request.query_params.get("Signature", [""])[0]
            expires = request.query_params.get("Expires", [""])[0]

            secret_key = self.credentials_store.get(access_key)
            if not secret_key:
                return False, None, f"Unknown access key: {access_key}"

            credentials = S3Credentials(
                access_key=access_key,
                secret_key=secret_key,
                region="us-east-1",
            )

            expiry_time = datetime.fromtimestamp(int(expires), tz=UTC)
            if datetime.now(UTC) > expiry_time:
                return False, credentials, "Presigned URL expired"

            string_to_sign = f"{request.method}\n\n\n{expires}\n{path}"
            calculated_sig = base64.b64encode(
                hmac.new(secret_key.encode(), string_to_sign.encode(), hashlib.sha1).digest()
            ).decode()

            if hmac.compare_digest(calculated_sig, signature):
                return True, credentials, ""
            return False, credentials, "Signature mismatch"

        except (KeyError, ValueError) as e:
            return False, None, f"Invalid V2 presigned URL: {e}"

    def _build_canonical_request(
        self, request: ParsedRequest, path: str, signed_headers: list[str]
    ) -> str:
        """Build canonical request for signature verification."""
        method = request.method.upper()
        canonical_uri = self._normalize_uri(path or "/")
        canonical_query = self._build_canonical_query_string(request.query_params)

        canonical_headers = ""
        for header in sorted(signed_headers):
            value = request.headers.get(header.lower(), "")
            # AWS SigV4 spec: trim leading/trailing whitespace and
            # collapse sequential spaces to single space
            normalized_value = re.sub(r"\s+", " ", value.strip())
            canonical_headers += f"{header.lower()}:{normalized_value}\n"

        signed_headers_str = ";".join(sorted(signed_headers))

        payload_hash = request.headers.get(
            "x-amz-content-sha256", hashlib.sha256(request.body).hexdigest()
        )

        return "\n".join(
            [
                method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers_str,
                payload_hash,
            ]
        )

    def _build_canonical_request_presigned(
        self,
        request: ParsedRequest,
        path: str,
        signed_headers: list[str],
        query_params: dict[str, list[str]],
    ) -> str:
        """Build canonical request for presigned URL verification."""
        method = request.method.upper()
        canonical_uri = self._normalize_uri(path or "/")
        canonical_query = self._build_canonical_query_string(query_params)

        canonical_headers = ""
        header_debug = {}
        for header in sorted(signed_headers):
            value = request.headers.get(header.lower(), "")
            # AWS SigV4 spec: trim leading/trailing whitespace and
            # collapse sequential spaces to single space
            normalized_value = re.sub(r"\s+", " ", value.strip())
            canonical_headers += f"{header.lower()}:{normalized_value}\n"
            header_debug[header] = normalized_value

        logger.debug(
            "Building presigned canonical request",
            method=method,
            canonical_uri=canonical_uri,
            signed_headers=signed_headers,
            header_values=header_debug,
        )

        signed_headers_str = ";".join(sorted(signed_headers))
        payload_hash = "UNSIGNED-PAYLOAD"

        return "\n".join(
            [
                method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_headers_str,
                payload_hash,
            ]
        )

    def _build_canonical_query_string(self, query_params: dict[str, list[str]]) -> str:
        """Build canonical query string with proper URL encoding for SigV4."""
        if not query_params:
            return ""

        sorted_params = []
        for key in sorted(query_params.keys()):
            for value in sorted(query_params[key]):
                # URL-encode key and value per AWS SigV4 spec
                # Use safe='' to encode everything except unreserved chars
                encoded_key = quote(key, safe="-_.~")
                encoded_value = quote(value, safe="-_.~")
                sorted_params.append((encoded_key, encoded_value))

        return "&".join(f"{k}={v}" for k, v in sorted_params)

    def _normalize_uri(self, path: str) -> str:
        """Normalize URI path for SigV4 canonical request.

        AWS SigV4 requires the URI to be URI-encoded. For S3, we preserve
        the existing encoding as-is, only normalizing to AWS SigV4 format.

        If the path is raw (already percent-encoded), we use it directly.
        If the path is decoded, we re-encode it.
        """
        if not path or path == "/":
            return "/"

        # Check if path appears to be already percent-encoded
        # by looking for valid %XX sequences
        has_encoding = bool(re.search(r"%[0-9A-Fa-f]{2}", path))

        if has_encoding:
            # Path is already encoded - normalize encoding format
            # Decode first, then re-encode to ensure consistent format
            # But preserve %2F (encoded slash) by not splitting on decoded /
            # We need to handle each segment between actual path separators

            # First, temporarily replace %2F with a placeholder to preserve it
            preserved = path.replace("%2F", "\x00SLASH\x00").replace("%2f", "\x00SLASH\x00")
            decoded = unquote(preserved)

            # Split by '/' (actual path separators)
            segments = decoded.split("/")
            encoded_segments = []
            for segment in segments:
                if segment:
                    # Re-encode, then restore preserved slashes
                    encoded = quote(segment, safe="-_.~")
                    encoded = encoded.replace("\x00SLASH\x00", "%2F")
                    encoded_segments.append(encoded)
                else:
                    encoded_segments.append("")

            return "/".join(encoded_segments) or "/"
        else:
            # Path is not encoded - encode it now
            segments = path.split("/")
            encoded_segments = []
            for segment in segments:
                if segment:
                    encoded_segments.append(quote(segment, safe="-_.~"))
                else:
                    encoded_segments.append("")

            return "/".join(encoded_segments) or "/"

    def _build_string_to_sign(
        self,
        amz_date: str,
        date_stamp: str,
        region: str,
        service: str,
        canonical_request: str,
    ) -> str:
        """Build string to sign."""
        credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
        canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

        return "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                canonical_request_hash,
            ]
        )
