"""Out-of-order multipart uploads must assemble in client-part order.

Regression for production pg_basebackup uploads that failed CompleteMultipartUpload
with InvalidPart: the short last part (one internal part) reached the proxy before
part 1 (two internal parts), so both were given internal part number 2.
"""

import hashlib
import itertools
import random
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import boto3
import pytest

from .conftest import run_s3proxy

pytestmark = [pytest.mark.e2e, pytest.mark.xdist_group("part_placement")]

MiB = 1024 * 1024
BUCKET = "test-part-placement"


@pytest.fixture(scope="module")
def s3():
    with run_s3proxy(4471, log_output=False) as (endpoint, _):
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1",
        )
        client.create_bucket(Bucket=BUCKET)
        yield client
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
            objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objects:
                client.delete_objects(Bucket=BUCKET, Delete={"Objects": objects})


def _payloads(sizes: list[int], seed: int) -> dict[int, bytes]:
    rng = random.Random(seed)
    return {n: rng.randbytes(size) for n, size in enumerate(sizes, start=1)}


def _upload(s3, key: str, payloads: dict[int, bytes], order: list[int]) -> str:
    """Upload parts in the given order, complete, return sha256 of the stored object."""
    upload_id = s3.create_multipart_upload(Bucket=BUCKET, Key=key)["UploadId"]
    etags = {}
    for n in order:
        etags[n] = s3.upload_part(
            Bucket=BUCKET, Key=key, UploadId=upload_id, PartNumber=n, Body=payloads[n]
        )["ETag"]
    s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": n, "ETag": etags[n]} for n in sorted(etags)]},
    )
    return hashlib.sha256(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()).hexdigest()


def _expected(payloads: dict[int, bytes]) -> str:
    return hashlib.sha256(b"".join(payloads[n] for n in sorted(payloads))).hexdigest()


def test_short_last_part_before_multi_internal_first_part(s3):
    payloads = _payloads([16 * MiB, 7 * MiB], seed=1)
    assert _upload(s3, "last-first", payloads, order=[2, 1]) == _expected(payloads)


@pytest.mark.parametrize("order", list(itertools.permutations([1, 2, 3])))
def test_every_arrival_order_of_three_parts(s3, order):
    payloads = _payloads([16 * MiB, 16 * MiB, 6 * MiB], seed=2)
    key = "order-" + "".join(map(str, order))
    assert _upload(s3, key, payloads, order=list(order)) == _expected(payloads)


def test_single_internal_parts_arriving_in_reverse_stay_dense(s3):
    payloads = _payloads([6 * MiB] * 5 + [1 * MiB], seed=3)
    assert _upload(s3, "reverse-small", payloads, order=[6, 5, 4, 3, 2, 1]) == _expected(payloads)


@pytest.mark.parametrize("round_", range(3))
def test_concurrent_parts_like_a_threaded_client(s3, round_):
    payloads = _payloads([16 * MiB] * 7 + [7 * MiB], seed=10 + round_)
    key = f"concurrent-{round_}"
    upload_id = s3.create_multipart_upload(Bucket=BUCKET, Key=key)["UploadId"]
    start = Barrier(len(payloads))

    def put(n: int) -> tuple[int, str]:
        start.wait()
        return n, s3.upload_part(
            Bucket=BUCKET, Key=key, UploadId=upload_id, PartNumber=n, Body=payloads[n]
        )["ETag"]

    with ThreadPoolExecutor(len(payloads)) as pool:
        etags = dict(pool.map(put, payloads))
    s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": n, "ETag": etags[n]} for n in sorted(etags)]},
    )
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    assert hashlib.sha256(body).hexdigest() == _expected(payloads)
