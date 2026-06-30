"""Self-check: listing timestamps use S3's 'Z' UTC suffix, not '+00:00'.

datetime.isoformat() emits '+00:00', which scylla-manager's rclone 1.51.0
rejects ("cannot parse '+00:00' as 'Z'"), failing the whole ListObjects. S3
itself returns millisecond-precision RFC3339 with a 'Z' suffix.
"""

from datetime import UTC, datetime, timezone

from s3proxy.handlers.buckets import _s3_timestamp


def test_utc_datetime_uses_z_suffix_millis():
    d = datetime(2026, 6, 30, 10, 24, 43, 399000, tzinfo=UTC)
    assert _s3_timestamp(d) == "2026-06-30T10:24:43.399Z"


def test_non_utc_is_converted_to_utc_z():
    from datetime import timedelta

    d = datetime(2026, 6, 30, 12, 24, 43, 0, tzinfo=timezone(timedelta(hours=2)))
    assert _s3_timestamp(d) == "2026-06-30T10:24:43.000Z"
    assert "+" not in _s3_timestamp(d)


def test_non_datetime_passthrough():
    assert _s3_timestamp("") == ""
    assert _s3_timestamp("already-a-string") == "already-a-string"


if __name__ == "__main__":
    test_utc_datetime_uses_z_suffix_millis()
    test_non_utc_is_converted_to_utc_z()
    test_non_datetime_passthrough()
    print("ok")
