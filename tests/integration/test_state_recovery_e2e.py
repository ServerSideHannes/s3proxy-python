"""E2E tests for state recovery from Redis loss.

These tests verify the state recovery fix works by:
1. Starting s3proxy with real Redis
2. Uploading parts
3. Deleting Redis state mid-upload
4. Verifying recovery reconstructs state from S3
5. Completing upload successfully
"""

import pytest
import redis.asyncio as redis


@pytest.mark.ha  # Requires actual Redis, not in-memory storage
class TestStateRecoveryWithRedis:
    """Tests that actually manipulate Redis state (requires Redis running)."""

    @pytest.fixture
    async def redis_client(self):
        """Create Redis client for state manipulation.

        Connects to real Redis at localhost:6379 for HA tests.
        """
        import socket

        # Check if Redis is running
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(("localhost", 6379))
            sock.close()
            if result != 0:
                pytest.skip("Redis not running on localhost:6379")
        except Exception:
            pytest.skip("Cannot connect to Redis")

        # Connect to real Redis
        client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        yield client
        await client.aclose()

    @pytest.mark.asyncio
    async def test_manual_redis_state_deletion(self, s3_client, test_bucket, redis_client):
        """Test recovery by manually deleting Redis state mid-upload.

        This is the MOST realistic test of the state recovery fix.
        """
        key = "test-manual-redis-deletion.bin"

        # Step 1: Upload part 1
        response = s3_client.create_multipart_upload(Bucket=test_bucket, Key=key)
        upload_id = response["UploadId"]

        part1_data = b"X" * 5_242_880
        resp1 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1_data
        )
        etag1 = resp1["ETag"]

        # Step 2: Delete Redis state (simulate TTL/restart/eviction)
        redis_key = f"s3proxy:upload:{test_bucket}:{key}:{upload_id}"
        await redis_client.delete(redis_key)

        # Step 3: Upload part 2 (triggers recovery if state was deleted)
        part2_data = b"Y" * 5_242_880
        resp2 = s3_client.upload_part(
            Bucket=test_bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=part2_data
        )
        etag2 = resp2["ETag"]

        # Step 4: Complete - should work because state was recovered
        s3_client.complete_multipart_upload(
            Bucket=test_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": etag1},
                    {"PartNumber": 2, "ETag": etag2},
                ]
            },
        )

        # Step 5: Verify both parts are present
        obj = s3_client.get_object(Bucket=test_bucket, Key=key)
        downloaded = obj["Body"].read()
        expected = part1_data + part2_data
        assert downloaded == expected
