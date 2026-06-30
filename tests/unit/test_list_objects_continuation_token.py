"""Self-check: V2 continuation tokens must not be URL-encoded.

barman-cloud / botocore send encoding-type=url on ListObjectsV2. Continuation
tokens are opaque cursors, not keys: per the S3 spec only Key/Prefix/Delimiter/
StartAfter are URL-encoded, and botocore never URL-decodes the token. When the
proxy URL-encoded a key-shaped token ('a/b/c.tar' -> 'a%2Fb%2Fc.tar') the token
could not round-trip, the backend kept returning the first page, and botocore
aborted the paginator with "The same next token was received twice".

This proves the V2 serializer emits the token verbatim (XML-escaped only) even
under encoding-type=url, so it survives the round-trip and pagination advances.
"""

from xml.etree.ElementTree import fromstring

from s3proxy.xml_responses import list_objects

TOKEN = "production-v3/production/base/20260619T223000/data_0007.tar"
_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def _parse(xml: str) -> dict:
    root = fromstring(xml)
    return {child.tag.replace(_NS, ""): child.text for child in root}


def test_v2_continuation_token_not_url_encoded():
    xml = list_objects(
        bucket="oceanio-dc2-postgresql-backups",
        prefix="production-v3/production/base/",
        max_keys=1000,
        is_truncated=True,
        next_token=TOKEN,
        objects=[],
        continuation_token=TOKEN,
        encoding_type="url",  # what barman/botocore sends
    )
    fields = _parse(xml)

    # Verbatim token -> '/' preserved, not '%2F'. This is the actual bug guard.
    assert fields["NextContinuationToken"] == TOKEN
    assert fields["ContinuationToken"] == TOKEN
    assert "%2F" not in xml

    # Keys are still URL-encoded under encoding-type=url (regression guard).
    key_xml = list_objects(
        bucket="b",
        prefix="",
        max_keys=1000,
        is_truncated=False,
        next_token=None,
        objects=[
            {
                "key": "a/b/c.tar",
                "last_modified": "2026-06-24T09:00:00",
                "etag": "x",
                "size": 1,
                "storage_class": "STANDARD",
            }
        ],
        encoding_type="url",
    )
    assert "a%2Fb%2Fc.tar" in key_xml


if __name__ == "__main__":
    test_v2_continuation_token_not_url_encoded()
    print("ok")
