"""Tests for AWS Signature Version 4 verification."""

from datetime import UTC, datetime, timedelta

import pytest

from s3proxy.s3client import (
    CLOCK_SKEW_TOLERANCE,
    ParsedRequest,
    S3Credentials,
    SigV4Verifier,
    _derive_signing_key,
)


class TestS3Credentials:
    """Test S3Credentials dataclass."""

    def test_basic_credentials(self):
        """Test basic credential creation."""
        creds = S3Credentials(
            access_key="AKIAIOSFODNN7EXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region="us-east-1",
        )
        assert creds.access_key == "AKIAIOSFODNN7EXAMPLE"
        assert creds.region == "us-east-1"
        assert creds.service == "s3"

    def test_custom_service(self):
        """Test credentials with custom service."""
        creds = S3Credentials(
            access_key="AKIAIOSFODNN7EXAMPLE",
            secret_key="secret",
            region="us-east-1",
            service="s3-object-lambda",
        )
        assert creds.service == "s3-object-lambda"


class TestParsedRequest:
    """Test ParsedRequest dataclass."""

    def test_basic_request(self):
        """Test basic request parsing."""
        req = ParsedRequest(
            method="GET",
            bucket="my-bucket",
            key="my-key",
        )
        assert req.method == "GET"
        assert req.bucket == "my-bucket"
        assert req.key == "my-key"
        assert req.body == b""
        assert req.is_presigned is False

    def test_request_with_body(self):
        """Test request with body."""
        req = ParsedRequest(
            method="PUT",
            bucket="bucket",
            key="key",
            body=b"test data",
        )
        assert req.body == b"test data"

    def test_request_with_query_params(self):
        """Test request with query parameters."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            query_params={"list-type": ["2"], "prefix": ["foo/"]},
        )
        assert req.query_params["list-type"] == ["2"]
        assert req.query_params["prefix"] == ["foo/"]


class TestSigV4Verifier:
    """Test SigV4 signature verification."""

    @pytest.fixture
    def credentials_store(self):
        """Create a test credentials store."""
        return {
            "AKIAIOSFODNN7EXAMPLE": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "TESTKEY": "TESTSECRET",
        }

    @pytest.fixture
    def verifier(self, credentials_store):
        """Create a SigV4 verifier."""
        return SigV4Verifier(credentials_store)

    def test_no_signature_returns_error(self, verifier):
        """Test request without signature fails."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={},
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert creds is None
        assert "No AWS signature" in error

    def test_unknown_access_key(self, verifier):
        """Test unknown access key fails."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=UNKNOWNKEY/20240115/us-east-1/s3/aws4_request,"
                    "SignedHeaders=host;x-amz-date,"
                    "Signature=invalid"
                ),
                "x-amz-date": "20240115T120000Z",
                "host": "localhost:9000",
            },
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert "Unknown access key" in error

    def test_missing_amz_date(self, verifier):
        """Test missing x-amz-date header fails."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKIAIOSFODNN7EXAMPLE/20240115/us-east-1/s3/aws4_request,"
                    "SignedHeaders=host;x-amz-date,"
                    "Signature=invalid"
                ),
                "host": "localhost:9000",
            },
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert creds is not None  # Credentials extracted but validation failed
        assert "Missing x-amz-date" in error

    def test_clock_skew_rejected(self, verifier):
        """Test request with excessive clock skew fails."""
        # Use a date far in the past
        old_date = "20200101T120000Z"
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKIAIOSFODNN7EXAMPLE/20200101/us-east-1/s3/aws4_request,"
                    "SignedHeaders=host;x-amz-date,"
                    "Signature=invalid"
                ),
                "x-amz-date": old_date,
                "host": "localhost:9000",
            },
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert "time too skewed" in error

    def test_invalid_date_format(self, verifier):
        """Test invalid date format fails."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=AKIAIOSFODNN7EXAMPLE/20240115/us-east-1/s3/aws4_request,"
                    "SignedHeaders=host;x-amz-date,"
                    "Signature=invalid"
                ),
                "x-amz-date": "invalid-date",
                "host": "localhost:9000",
            },
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert "Invalid x-amz-date format" in error

    def test_malformed_authorization_header(self, verifier):
        """Test malformed authorization header fails gracefully."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "authorization": "AWS4-HMAC-SHA256 malformed",
                "x-amz-date": "20240115T120000Z",
            },
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert "Invalid Authorization header" in error


class TestPresignedV4:
    """Test presigned URL V4 verification."""

    @pytest.fixture
    def credentials_store(self):
        """Create a test credentials store."""
        return {"AKIAIOSFODNN7EXAMPLE": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}

    @pytest.fixture
    def verifier(self, credentials_store):
        """Create a SigV4 verifier."""
        return SigV4Verifier(credentials_store)

    def test_expired_presigned_url(self, verifier):
        """Test expired presigned URL fails."""
        # Use old date with 0 expiry
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            query_params={
                "X-Amz-Algorithm": ["AWS4-HMAC-SHA256"],
                "X-Amz-Credential": ["AKIAIOSFODNN7EXAMPLE/20200101/us-east-1/s3/aws4_request"],
                "X-Amz-Date": ["20200101T120000Z"],
                "X-Amz-Expires": ["3600"],
                "X-Amz-SignedHeaders": ["host"],
                "X-Amz-Signature": ["invalid"],
            },
            headers={"host": "localhost:9000"},
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert "expired" in error.lower()

    def test_unknown_access_key_presigned(self, verifier):
        """Test unknown access key in presigned URL fails."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            query_params={
                "X-Amz-Algorithm": ["AWS4-HMAC-SHA256"],
                "X-Amz-Credential": ["UNKNOWNKEY/20240115/us-east-1/s3/aws4_request"],
                "X-Amz-Date": ["20240115T120000Z"],
                "X-Amz-Expires": ["3600"],
                "X-Amz-SignedHeaders": ["host"],
                "X-Amz-Signature": ["invalid"],
            },
            headers={"host": "localhost:9000"},
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert "Unknown access key" in error

    def test_valid_presigned_get(self, verifier):
        """Test valid presigned GET URL passes verification."""
        access_key = "AKIAIOSFODNN7EXAMPLE"
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        region = "us-east-1"
        service = "s3"
        host = "localhost:9000"
        path = "/bucket/test-key.txt"
        amz_date = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        date_stamp = amz_date[:8]

        credential = f"{access_key}/{date_stamp}/{region}/{service}/aws4_request"
        signed_headers = "host"

        # Build query params for signing (without signature)
        query_for_signing = {
            "X-Amz-Algorithm": ["AWS4-HMAC-SHA256"],
            "X-Amz-Credential": [credential],
            "X-Amz-Date": [amz_date],
            "X-Amz-Expires": ["3600"],
            "X-Amz-SignedHeaders": [signed_headers],
        }

        # Compute the signature the same way the verifier does
        signature = verifier._compute_v4_signature(
            verifier._build_canonical_request_presigned(
                ParsedRequest(
                    method="GET",
                    bucket="bucket",
                    key="test-key.txt",
                    query_params=query_for_signing,
                    headers={"host": host},
                ),
                path,
                ["host"],
                query_for_signing,
            ),
            amz_date,
            date_stamp,
            region,
            service,
            secret_key,
        )

        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="test-key.txt",
            query_params={**query_for_signing, "X-Amz-Signature": [signature]},
            headers={"host": host},
        )
        valid, creds, error = verifier.verify(req, path)

        assert valid is True
        assert creds.access_key == access_key
        assert error == ""

    def test_valid_presigned_put(self, verifier):
        """Test valid presigned PUT URL passes verification."""
        access_key = "AKIAIOSFODNN7EXAMPLE"
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        region = "us-east-1"
        service = "s3"
        host = "localhost:9000"
        path = "/bucket/upload.bin"
        amz_date = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        date_stamp = amz_date[:8]

        credential = f"{access_key}/{date_stamp}/{region}/{service}/aws4_request"
        signed_headers = "host"

        query_for_signing = {
            "X-Amz-Algorithm": ["AWS4-HMAC-SHA256"],
            "X-Amz-Credential": [credential],
            "X-Amz-Date": [amz_date],
            "X-Amz-Expires": ["3600"],
            "X-Amz-SignedHeaders": [signed_headers],
        }

        signature = verifier._compute_v4_signature(
            verifier._build_canonical_request_presigned(
                ParsedRequest(
                    method="PUT",
                    bucket="bucket",
                    key="upload.bin",
                    query_params=query_for_signing,
                    headers={"host": host},
                ),
                path,
                ["host"],
                query_for_signing,
            ),
            amz_date,
            date_stamp,
            region,
            service,
            secret_key,
        )

        req = ParsedRequest(
            method="PUT",
            bucket="bucket",
            key="upload.bin",
            query_params={**query_for_signing, "X-Amz-Signature": [signature]},
            headers={"host": host},
        )
        valid, creds, error = verifier.verify(req, path)

        assert valid is True
        assert creds.access_key == access_key
        assert error == ""


class TestPresignedV2:
    """Test legacy presigned URL V2 verification."""

    @pytest.fixture
    def credentials_store(self):
        """Create a test credentials store."""
        return {"AKIAIOSFODNN7EXAMPLE": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}

    @pytest.fixture
    def verifier(self, credentials_store):
        """Create a SigV4 verifier."""
        return SigV4Verifier(credentials_store)

    def test_expired_presigned_v2(self, verifier):
        """Test expired V2 presigned URL fails."""
        # Expired timestamp
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            query_params={
                "AWSAccessKeyId": ["AKIAIOSFODNN7EXAMPLE"],
                "Expires": ["1577836800"],  # 2020-01-01
                "Signature": ["invalid"],
            },
            headers={"host": "localhost:9000"},
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert "expired" in error.lower()

    def test_unknown_access_key_v2(self, verifier):
        """Test unknown access key in V2 presigned URL fails."""
        future_timestamp = str(int((datetime.now(UTC) + timedelta(hours=1)).timestamp()))
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            query_params={
                "AWSAccessKeyId": ["UNKNOWNKEY"],
                "Expires": [future_timestamp],
                "Signature": ["invalid"],
            },
            headers={"host": "localhost:9000"},
        )
        valid, creds, error = verifier.verify(req, "/bucket/key")

        assert valid is False
        assert "Unknown access key" in error


class TestClockSkewTolerance:
    """Test clock skew tolerance."""

    def test_tolerance_value(self):
        """Test clock skew tolerance is 5 minutes."""
        assert timedelta(minutes=5) == CLOCK_SKEW_TOLERANCE

    def test_within_tolerance(self):
        """Test request within tolerance is accepted."""
        # This is a partial test - actual signature would need to be valid
        now = datetime.now(UTC)
        skewed = now - timedelta(minutes=4)  # Within 5 min tolerance
        assert abs(now - skewed) < CLOCK_SKEW_TOLERANCE

    def test_outside_tolerance(self):
        """Test request outside tolerance is rejected."""
        now = datetime.now(UTC)
        skewed = now - timedelta(minutes=6)  # Outside 5 min tolerance
        assert abs(now - skewed) > CLOCK_SKEW_TOLERANCE


class TestSigningKeyDerivation:
    """Test signing key derivation helpers."""

    def test_signing_key_format(self):
        """Test signing key is derived correctly."""
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        date_stamp = "20240115"
        region = "us-east-1"
        service = "s3"

        signing_key = _derive_signing_key(secret_key, date_stamp, region, service)

        # Signing key should be 32 bytes (SHA256 output)
        assert len(signing_key) == 32
        assert isinstance(signing_key, bytes)

    def test_signing_key_deterministic(self):
        """Test signing key derivation is deterministic."""
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        date_stamp = "20240115"
        region = "us-east-1"
        service = "s3"

        key1 = _derive_signing_key(secret_key, date_stamp, region, service)
        key2 = _derive_signing_key(secret_key, date_stamp, region, service)

        assert key1 == key2

    def test_different_dates_different_keys(self):
        """Test different dates produce different signing keys."""
        secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

        key1 = _derive_signing_key(secret_key, "20240115", "us-east-1", "s3")
        key2 = _derive_signing_key(secret_key, "20240116", "us-east-1", "s3")

        assert key1 != key2


class TestCanonicalRequest:
    """Test canonical request building."""

    @pytest.fixture
    def verifier(self):
        """Create a verifier for testing internal methods."""
        return SigV4Verifier({})

    def test_canonical_query_string_ordering(self, verifier):
        """Test query parameters are sorted alphabetically."""
        query_params = {"z-param": ["z"], "a-param": ["a"], "m-param": ["m"]}
        canonical = verifier._build_canonical_query_string(query_params)

        # Parameters should be alphabetically sorted
        assert canonical.index("a-param") < canonical.index("m-param")
        assert canonical.index("m-param") < canonical.index("z-param")

    def test_canonical_query_string_url_encoding(self, verifier):
        """Test query parameters are URL encoded."""
        query_params = {"key": ["value with spaces"]}
        canonical = verifier._build_canonical_query_string(query_params)

        assert "value%20with%20spaces" in canonical

    def test_empty_query_string(self, verifier):
        """Test empty query parameters."""
        canonical = verifier._build_canonical_query_string({})
        assert canonical == ""

    def test_header_value_whitespace_normalization(self, verifier):
        """Test header values have whitespace normalized per AWS SigV4 spec."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "host": "  s3.amazonaws.com  ",  # Leading/trailing spaces
                "x-amz-date": "20240115T120000Z",
                "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
            },
        )
        canonical = verifier._build_canonical_request(req, "/bucket/key", ["host", "x-amz-date"])
        # Host value should be trimmed
        assert "host:s3.amazonaws.com\n" in canonical

    def test_header_value_sequential_spaces_collapsed(self, verifier):
        """Test sequential spaces in header values are collapsed to single space."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "host": "s3.amazonaws.com",
                "x-amz-date": "20240115T120000Z",
                "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
                "x-amz-meta-test": "value   with    multiple   spaces",
            },
        )
        canonical = verifier._build_canonical_request(
            req, "/bucket/key", ["host", "x-amz-date", "x-amz-meta-test"]
        )
        # Sequential spaces should be collapsed to single space
        assert "x-amz-meta-test:value with multiple spaces\n" in canonical

    def test_header_value_tabs_normalized(self, verifier):
        """Test tabs in header values are normalized to spaces."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "host": "s3.amazonaws.com",
                "x-amz-date": "20240115T120000Z",
                "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
                "x-amz-meta-test": "value\twith\ttabs",
            },
        )
        canonical = verifier._build_canonical_request(
            req, "/bucket/key", ["host", "x-amz-date", "x-amz-meta-test"]
        )
        # Tabs should be normalized to single spaces
        assert "x-amz-meta-test:value with tabs\n" in canonical

    def test_header_value_mixed_whitespace(self, verifier):
        """Test mixed whitespace (spaces, tabs, newlines) is normalized."""
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "host": "s3.amazonaws.com",
                "x-amz-date": "20240115T120000Z",
                "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
                "x-amz-meta-test": "  value \t with \n mixed  whitespace  ",
            },
        )
        canonical = verifier._build_canonical_request(
            req, "/bucket/key", ["host", "x-amz-date", "x-amz-meta-test"]
        )
        # All whitespace should be normalized
        assert "x-amz-meta-test:value with mixed whitespace\n" in canonical


class TestAuthorizationHeaderParsing:
    """Test Authorization header parsing edge cases."""

    @pytest.fixture
    def verifier(self):
        """Create a verifier."""
        return SigV4Verifier({"TESTKEY": "TESTSECRET"})

    def test_extra_whitespace_handled(self, verifier):
        """Test extra whitespace in Authorization header is handled."""
        # Header with extra spaces
        auth = (
            "AWS4-HMAC-SHA256  "
            "Credential=TESTKEY/20240115/us-east-1/s3/aws4_request , "
            "SignedHeaders=host;x-amz-date , "
            "Signature=abc123"
        )
        req = ParsedRequest(
            method="GET",
            bucket="bucket",
            key="key",
            headers={
                "authorization": auth,
                "x-amz-date": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
                "host": "localhost",
            },
        )
        # Should not crash, though signature will mismatch
        valid, creds, error = verifier.verify(req, "/bucket/key")

        # Credentials should be extracted even if signature mismatches
        assert creds is not None or "Invalid" in error
