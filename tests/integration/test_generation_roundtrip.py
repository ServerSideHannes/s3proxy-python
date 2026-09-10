"""Real S3/HTTP coverage for v3 generation publication and staged assembly."""

import os
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from .conftest import _find_free_port, minio_backend, run_s3proxy

pytestmark = pytest.mark.e2e


@pytest.mark.parametrize(
    "redis_url",
    [""]
    + ([os.environ["S3PROXY_TEST_REDIS_URL"]] if os.environ.get("S3PROXY_TEST_REDIS_URL") else []),
)
def test_generation_roundtrip(redis_url):
    with (
        minio_backend() as backend,
        run_s3proxy(
            _find_free_port(),
            S3PROXY_HOST=backend,
            S3PROXY_MEMORY_LIMIT_MB="64",
            S3PROXY_REDIS_URL=redis_url,
            log_output=True,
        ) as (endpoint, _),
    ):
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
            config=Config(
                s3={"addressing_style": "path"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        bucket = "generation-" + uuid.uuid4().hex[:16]
        client.create_bucket(Bucket=bucket)
        upload = client.create_multipart_upload(
            Bucket=bucket, Key="object", Metadata={"owner": "test"}
        )["UploadId"]
        tail = client.upload_part(
            Bucket=bucket, Key="object", UploadId=upload, PartNumber=2, Body=b"tail"
        )
        data = b"A" * (9 * 1024**2)
        first = client.upload_part(
            Bucket=bucket, Key="object", UploadId=upload, PartNumber=1, Body=data
        )
        pending = client.list_parts(Bucket=bucket, Key="object", UploadId=upload, MaxParts=1)
        assert pending["IsTruncated"] and pending["Parts"][0]["Size"] == len(data)
        assert pending["Parts"][0]["ETag"] == first["ETag"]
        second_page = client.list_parts(
            Bucket=bucket, Key="object", UploadId=upload, PartNumberMarker=1
        )
        assert second_page["Parts"][0]["Size"] == 4
        parts = [{"PartNumber": 1, "ETag": first["ETag"]}, {"PartNumber": 2, "ETag": tail["ETag"]}]
        complete = client.complete_multipart_upload(
            Bucket=bucket, Key="object", UploadId=upload, MultipartUpload={"Parts": parts}
        )
        head = client.head_object(Bucket=bucket, Key="object")
        get = client.get_object(Bucket=bucket, Key="object")
        assert get["Body"].read() == data + b"tail"
        assert head["Metadata"] == {"owner": "test"}
        assert head["ETag"] == get["ETag"] == complete["ETag"]
        listed = client.list_objects_v2(Bucket=bucket)["Contents"]
        assert len(listed) == 1 and listed[0]["ETag"] == complete["ETag"]
        ranged = client.get_object(Bucket=bucket, Key="object", Range=f"bytes={len(data) - 2}-")
        assert ranged["Body"].read() == b"AAtail"
        client.complete_multipart_upload(
            Bucket=bucket, Key="object", UploadId=upload, MultipartUpload={"Parts": parts}
        )
        with pytest.raises(ClientError):
            client.complete_multipart_upload(
                Bucket=bucket,
                Key="object",
                UploadId="wrong-upload",
                MultipartUpload={"Parts": parts},
            )
        client.copy_object(
            Bucket=bucket, Key="native", CopySource={"Bucket": bucket, "Key": "object"}
        )
        assert client.get_object(Bucket=bucket, Key="native")["Body"].read() == data + b"tail"
        assert client.head_object(Bucket=bucket, Key="native")["Metadata"] == {"owner": "test"}
        client.put_object(Bucket=bucket, Key="object", Body=b"new", Metadata={"owner": "new"})
        assert client.get_object(Bucket=bucket, Key="object")["Body"].read() == b"new"
        with pytest.raises(ClientError) as error:
            client.put_object(Bucket=bucket, Key="object", Body=b"rejected", IfNoneMatch="*")
        assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        assert client.get_object(Bucket=bucket, Key="object")["Body"].read() == b"new"
        # Exercise the shared staging pipeline for server-side copy parts.
        upload = client.create_multipart_upload(Bucket=bucket, Key="copy")["UploadId"]
        part = client.upload_part_copy(
            Bucket=bucket,
            Key="copy",
            UploadId=upload,
            PartNumber=1,
            CopySource={"Bucket": bucket, "Key": "object"},
        )
        client.complete_multipart_upload(
            Bucket=bucket,
            Key="copy",
            UploadId=upload,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["CopyPartResult"]["ETag"]}]},
        )
        assert client.get_object(Bucket=bucket, Key="copy")["Body"].read() == b"new"

        # A raw source copied over a previously encrypted destination must ignore old sidecars.
        raw = boto3.client(
            "s3",
            endpoint_url=backend,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )
        raw.put_object(Bucket=bucket, Key="raw", Body=b"plain")
        client.copy_object(Bucket=bucket, Key="native", CopySource={"Bucket": bucket, "Key": "raw"})
        assert client.get_object(Bucket=bucket, Key="native")["Body"].read() == b"plain"
        # Fault injection against real HTTP at the buffering boundary.
        import hashlib

        import requests
        from botocore.auth import S3SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        for length in (8 * 1024**2 - 1, 8 * 1024**2, 8 * 1024**2 + 1):
            good = b"a" * length
            signed = AWSRequest(
                method="PUT",
                url=f"{endpoint}/{bucket}/object",
                data=good,
                headers={"x-amz-content-sha256": hashlib.sha256(good).hexdigest()},
            )
            S3SigV4Auth(Credentials("minioadmin", "minioadmin"), "s3", "us-east-1").add_auth(signed)
            result = requests.put(
                signed.url, headers=dict(signed.headers), data=b"b" * length, timeout=15
            )
            assert result.status_code == 403
            assert client.get_object(Bucket=bucket, Key="object")["Body"].read() == b"new"
        # This test owns a unique bucket and removes only that bucket's data.
        objects = raw.list_objects_v2(Bucket=bucket).get("Contents", [])
        if objects:
            raw.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in objects]}
            )
        raw.delete_bucket(Bucket=bucket)
