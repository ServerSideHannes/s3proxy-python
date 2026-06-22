"""UploadPart path selection — guards the Elasticsearch-snapshot OOM regression.

ES sends 16MB *signed* (real SHA256, not UNSIGNED/STREAMING/aws-chunked) parts.
Those must take the framed O(frame)-memory path, not buffer the whole part.
"""

from s3proxy.handlers.multipart.upload_part import classify_upload

MB = 1024 * 1024
SIGNED_SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_signed_16mb_part_uses_framed_path():
    """The exact ES case: 16MB signed, non-chunked -> framed, not buffered."""
    c = classify_upload(SIGNED_SHA, "", 16 * MB)
    assert c.use_framed is True
    assert c.is_large_signed is True
    assert c.needs_chunked_decode is False
    assert c.is_unsigned is False


def test_small_signed_part_uses_framed_path():
    """Even a sub-32MB signed part streams now (used to buffer below the old threshold)."""
    assert classify_upload(SIGNED_SHA, "", 5 * MB).use_framed is True


def test_unsigned_large_part_uses_framed_path():
    c = classify_upload("UNSIGNED-PAYLOAD", "", 256 * MB)
    assert c.use_framed is True and c.is_unsigned is True


def test_streaming_sig_uses_buffered_path():
    """Streaming signature length is unknown up front -> buffered/chunked decode."""
    c = classify_upload("STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "", 16 * MB)
    assert c.use_framed is False
    assert c.needs_chunked_decode is True


def test_aws_chunked_uses_buffered_path():
    c = classify_upload(SIGNED_SHA, "aws-chunked", 16 * MB)
    assert c.use_framed is False and c.needs_chunked_decode is True


def test_zero_length_never_framed():
    assert classify_upload(SIGNED_SHA, "", 0).use_framed is False
